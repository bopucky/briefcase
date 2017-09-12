/*
 * Copyright (C) 2011 University of Washington.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.briefcase.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bushe.swing.event.EventBus;
import org.javarosa.core.model.instance.AbstractTreeElement;
import org.javarosa.core.model.instance.TreeElement;
import org.kxml2.kdom.Document;
import org.kxml2.kdom.Element;
import org.kxml2.kdom.Node;
import org.opendatakit.briefcase.model.BriefcaseFormDefinition;
import org.opendatakit.briefcase.model.CryptoException;
import org.opendatakit.briefcase.model.ExportProgressEvent;
import org.opendatakit.briefcase.model.ExportProgressPercentageEvent;
import org.opendatakit.briefcase.model.FileSystemException;
import org.opendatakit.briefcase.model.ParsingException;
import org.opendatakit.briefcase.model.TerminationFuture;
import org.opendatakit.briefcase.util.XmlManipulationUtils.FormInstanceMetadata;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** STATA export related imports**/
//import com.sun.javaws.jnl.XMLFormat;
//import sun.text.normalizer.UCharacter;
import org.javarosa.core.model.FormDef;
import org.javarosa.core.model.QuestionDef;
import org.javarosa.core.model.SelectChoice;
import java.lang.reflect.Method;
import java.io.StringWriter;
import org.kxml2.io.KXmlSerializer;
import org.javarosa.xform.parse.XFormParser;
import java.lang.Math;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.io.FileNotFoundException;

public class ExportToDta implements ITransformFormAction {

  private static final String MEDIA_DIR = "media";

  //static final Logger log = Logger.getLogger(ExportToDta.class.getName());
  private static final Log log = LogFactory.getLog(ExportToCsv.class);

  File outputDir;
  File outputMediaDir;
  String baseFilename;
  BriefcaseFormDefinition briefcaseLfd;
  TerminationFuture terminationFuture;
  Map<TreeElement, OutputStreamWriter> fileMap = new HashMap<TreeElement, OutputStreamWriter>();

  boolean exportMedia = true;
  Date startDate;
  Date endDate;
  boolean overwrite = true;
  int totalFilesSkipped = 0;
  int totalInstances = 0;
  int processedInstances = 0;
  //XMLFormat xmlFormat;

  /**
   * dta specific structures
   */
  // default variable Stata data type
  // stata float type
  private static final String default_type = "double";
  // stata formatting for float
  private static final String default_fmt = "%15.0g";
  // Header related
  public String header = "";
  public String ds_format = "113";
  public String filetype = "1";
  private int maxVarNameLength = 29;
  public int nvar = 0;
  public int nobs = 0;
  // put whitespace so that long <data_label> tag and <timestamp> tag is created during write().
  // Stata does not like short tags
  String data_label = " ";
  String time_stamp = " ";
  public int dta_idx = 0;
  public int descriptors_idx = 0;
  public int variable_labels_idx = 0;
  public int data_idx = 0;
  public int value_labels_idx = 0;
  public boolean useShortVarName = true;
  private FormDef fd;
  Map<String,String> iTextMap;
  Map<String,String> typelistMap;
  Map<String,String> fmtlistMap;
  Map<String,String> variable_labelsMap;
  Map<TreeElement, Document> docMap = new HashMap<TreeElement, Document>();
  Map<Document, Map<String,String>> docVarsMap = new HashMap<Document, Map<String,String>>();
  //Map<Document, Map<String,Integer>> docVarsMap = new HashMap<Document, Map<String,Integer>>();
  Map<Element,List<StringBuilder>> valsMapBufs = new HashMap<Element,List<StringBuilder>>();
  Map<String,String> allVallabsMap = new HashMap<String,String>();
  //Map<String,String> repeatMap = new HashMap<String,String>();
  Map<String,Integer> repeatMap = new HashMap<String,Integer>();
  Map<String,Integer> repeatMapTmp = new HashMap<String,Integer>();
  // repeatVarsMap maps long/full variable name to short name
  Map<String,String> repeatVarsMap = new HashMap<String,String>();
  Map<String,Integer> allVallabsDone =  new HashMap<String,Integer>();
  Method methodGetChildren = null;
  Method methodSize = null;
  Method methodGetItem = null;
  private StringBuilder allVallabs = new StringBuilder();

  Element value_labels_all;

  // Default briefcase constructor.
  // Exports as .xml, an xml version of the .xml format
  public ExportToDta(File outputDir, BriefcaseFormDefinition lfd, TerminationFuture terminationFuture) {
    this(outputDir, lfd, terminationFuture, lfd.getFormName(), true, false, null, null);
  }

  public ExportToDta(File outputDir, BriefcaseFormDefinition lfd, TerminationFuture terminationFuture, String filename, boolean exportMedia, Boolean overwrite, Date start, Date end) {
     this.outputDir = outputDir;
     this.outputMediaDir = new File(outputDir, MEDIA_DIR);
     this.briefcaseLfd = lfd;
     this.terminationFuture = terminationFuture;

     // Strip .xml, it gets added later
     if (filename.endsWith(".xml")) {
         filename = filename.substring(0, filename.length()-4);
     }
     this.baseFilename = filename;
     this.exportMedia = exportMedia;
     //this.overwrite = overwrite;
     this.overwrite = true;
     this.startDate = start;
     this.endDate = end;
  }

  @Override
  public boolean doAction() {
    boolean allSuccessful = true;
    File instancesDir;
    try {
      instancesDir = FileSystemUtils.getFormInstancesDirectory(briefcaseLfd.getFormDirectory());
    } catch (FileSystemException e) {
      // emit status change...
      EventBus.publish(new ExportProgressEvent("Unable to access instances directory of form"));
      e.printStackTrace();
      return false;
    }

    if (!outputDir.exists()) {
      if (!outputDir.mkdir()) {
        EventBus.publish(new ExportProgressEvent("Unable to create destination directory"));
        return false;
      }
    }

    if (exportMedia) {
       if (!outputMediaDir.exists()) {
         if (!outputMediaDir.mkdir()) {
           EventBus
               .publish(new ExportProgressEvent("Unable to create destination media directory"));
           return false;
         }
       }
    }

    if (!processFormDefinition()) {
      // weren't able to initialize the dta file...
      return false;
    }

    File[] instances = instancesDir.listFiles();

    for (File instanceDir : instances) {
      if ( terminationFuture.isCancelled() ) {
        EventBus.publish(new ExportProgressEvent("ABORTED"));
        allSuccessful = false;
        break;
      }
      if (instanceDir.getName().startsWith("."))
        continue; // Mac OSX
      allSuccessful = allSuccessful && processInstance(instanceDir);
    }

    // Process <itext>
    iTextMap = processItext();
    // Print each document using it's associated writer
    for (Map.Entry<TreeElement, Document> adoc : docMap.entrySet()) {
      try {
        // The writer and the_doc are associated with the same key (a TreeElement)
        OutputStreamWriter w = fileMap.get(adoc.getKey());
        StringWriter fo = new StringWriter();

        // Get the document
        Document the_doc = adoc.getValue();
        // Write to xml file
        KXmlSerializer serializer = new KXmlSerializer();

        // Build <descriptors> tree
        allSuccessful = allSuccessful && getQuestions(fd, adoc.getValue());
        allSuccessful = allSuccessful && processDtaDescriptors(adoc.getValue());

        // ALT: write in smaller chunks to ease up on memory usage
        allSuccessful = allSuccessful && writeSpecial(the_doc, w, fo, serializer);

        // Close writers
        fo.close();
        w.close();
        // Cleanup
        the_doc = null;
      } catch (IOException e) {
        e.printStackTrace();
        EventBus.publish(new ExportProgressEvent("Error flushing dta file"));
        allSuccessful = false;
      }
    }

    return allSuccessful;
  }

  private boolean writeSpecial(Document the_doc, OutputStreamWriter w, StringWriter fo, KXmlSerializer serializer){
    try {
      KXmlSerializer serializer2 = new KXmlSerializer();
      serializer.setOutput(w);
      the_doc.setEncoding("UTF-8");

      // Write xml header
      serializer.startDocument("UTF-8",false);
      // Write <dta>
      w.write("<dta>");

      // Write <header>
      the_doc.getElement(null,"dta").getElement(null,"header").write(serializer);
      w.flush();

      // Write <descriptors>
      writeSpecialManual(the_doc,w);
      w.flush();

      /*
      // Now write <data> xml fragments
      processDtaData(the_doc, w);
      w.flush();


      // Now print allVallabs
      w.write("<value_labels>");
      w.write(allVallabs.toString());
      w.write("</value_labels>");
      */

      // Write </dta> close tag
      w.write("<dta>");
      w.flush();

    } catch (IOException e) {
      e.printStackTrace();
      EventBus.publish(new ExportProgressEvent("Error flushing dta file"));
      return false;
    }
    return true;
  }

  private void writeSpecialManual(Document the_doc, OutputStreamWriter w){
    StringBuffer typelist= new StringBuffer();
    StringBuffer varlist= new StringBuffer();
    StringBuffer srtlist= new StringBuffer();
    StringBuffer fmtlist= new StringBuffer();
    StringBuffer lbllist= new StringBuffer();
    StringBuffer data= new StringBuffer();
    StringBuffer variable_labels= new StringBuffer();
    StringBuffer question_label = new StringBuffer();

    Map<String,String> varsMap = docVarsMap.get(the_doc);
    //Map<String,Integer> varsMap = docVarsMap.get(the_doc);

    String the_type = null;
    String the_fmt = null;

    boolean res = false;

    // Open tags for tags in typelist,fmtlist,lbllist
    typelist.append("<typelist>");
    varlist.append("<varlist>");
    srtlist.append("<srtlist />");
    fmtlist.append("<fmtlist>");
    lbllist.append("<lbllist>");
    variable_labels.append("<variable_labels>");

    // Construct xml strings for tags in <descriptors>
    StringBuilder b = new StringBuilder();
    String new_varname = null;
    int ct=0;

    for (String varname : varsMap.keySet()) {
      // <typelist><type>..
      the_type = typelistMap.get(varname);
      the_type = the_type == null ? "str244" : the_type;

      if(useShortVarName){
        new_varname = repeatVarsMap.get(varname);
        // Create special short names for special repeat variables which are used for
        // multiple bind nodesets update counter in repeatMap to account for remaining
        // counts for special short names of special repeats
        if(new_varname!=null){
          ct = repeatMap.get(new_varname);
          new_varname = new_varname+"_00"+ct;
          ct--;
          repeatMap.put(repeatVarsMap.get(varname),ct);
          // Update entry in repeatVarsMap with its new name so we can reuse it
          // to update the associated <v> entries that use the longname etc.
          //repeatVarsMap.put(varname,new_varname);
        }else{
          // Get original variable name. This is the same as in the form
          new_varname = varsMap.get(varname);
        }
      }else{
        // keep long name
        new_varname = varname;
      }

      //b.append(makeStataVarName(varname));
      String stataVarName = makeStataVarName(new_varname);
      b.append(stataVarName);
      typelist.append("<type ").append("varname=").append("\"")
                .append(b.toString()).append("\"")
                .append(">");
        typelist.append(the_type);
        typelist.append("</type>");

        // <varlist><variable>..
        varlist.append("<variable ").append("varname=").append("\"")
                .append(b.toString()).append("\"")
                .append(" />");

      // <fmtlist><fmt>..
      the_fmt = fmtlistMap.get(varname);
      the_fmt = the_fmt==null ?  "%13s" : the_fmt;
      fmtlist.append("<fmt ").append("varname=").append("\"")
                .append(b.toString()).append("\"")
                .append(">");
        fmtlist.append(the_fmt);
        fmtlist.append("</fmt>");

      // <lbllist><lblname>..
        lbllist.append("<lblname ").append("varname=").append("\"")
                .append(b.toString()).append("\"")
                .append(">");
        lbllist.append(b.toString());
        lbllist.append("</lblname>");

      // <lbllist><vlabel>..
      question_label = variable_labelsMap.containsKey(varname)  ? question_label.append(variable_labelsMap.get(varname)): question_label.append(b.toString());
      variable_labels.append("<vlabel ").append("varname=").append("\"")
              .append(b.toString()).append("\"")
              .append(">");
      variable_labels.append(question_label);
      variable_labels.append("</vlabel>");
      question_label.delete(0,question_label.length());

      //<value_labels><vallab>...
      if(allVallabsMap.containsKey(varname) && !allVallabsDone.containsKey(new_varname)){
        allVallabs.append("<vallab").append(" ")
                .append("name=").append("\"").append(new_varname).append("\"")
                .append(">")
                .append(allVallabsMap.get(varname))
                .append("</vallab>");
        allVallabsDone.put(new_varname,null);

      }

      // clear this
      b.delete(0,b.length());

    }

    // Close container tags for typelist,fmtlist,lbllist
    typelist.append("</typelist>");
    varlist.append("</varlist>");
    fmtlist.append("</fmtlist>");
    lbllist.append("</lbllist>");
    variable_labels.append("</variable_labels>");

    // Write to output stream. The order of the elements is important
    try{
      w.append("<descriptors>");
      w.append(typelist).append(varlist).append(srtlist).append(fmtlist).append(lbllist);
      w.append("</descriptors>");
      w.append(variable_labels);
      w.flush();

      // Now write <data> xml fragments
      processDtaData(the_doc, w);
      w.flush();

      // Now print allVallabs
      w.write("<value_labels>");
      w.write(allVallabs.toString());
      w.write("</value_labels>");
      w.flush();

    }catch (IOException e){
      e.printStackTrace();
      log.info("Problems appending to writer");
    }

  }
  // Get all select choice values from model, and add as <label> to <vallab /> nodes in <value_labels />
  private boolean getQuestions(Object v, Document the_doc){
    Object result = null, item = null, resultsize = null;
    QuestionDef q = null;
    SelectChoice ch = null;
    List choices = null;
    Element vallab = null;
    String the_vallab = null, question_label = null, question_tid=null;
    String val = null, txt = null, tid = null, the_tid = null, the_key=null;
    String[] the_id = null;
    StringBuilder b = new StringBuilder();
    int sz = 0;
    boolean isQ = false;

    try {
      // Get children and size of result set the methods exist
      methodGetChildren = v.getClass().getMethod("getChildren", (Class<?>[]) null);
      result = methodGetChildren.invoke(v, (Object[]) null);
      methodSize = result.getClass().getMethod("size", (Class<?>[]) null);
      resultsize = methodSize.invoke(result, (Object[]) null);
      methodGetItem = result.getClass().getMethod("get",new Class[] {int.class});
    }catch(IllegalAccessException | InvocationTargetException | SecurityException | NoSuchMethodException n){
      n.printStackTrace();
      return false;
    }/*catch (IllegalAccessException | InvocationTargetException | SecurityException a){
      a.printStackTrace();
      return false;
    }*/

    try {
      if((Integer) resultsize >0){
        for(int i=0; i<(Integer) resultsize;i++){
          item = methodGetItem.invoke(result, i);
          isQ = item instanceof QuestionDef;
          if(isQ){
            q = (QuestionDef) item;
            choices = q.getChoices();
            //find parent <vallab> associated with this question
            sz = q.getBind().getReference().toString().split("/").length;
            the_vallab = q.getBind().getReference().toString();

            /*
            //the_vallab = q.getBind().getReference().toString().split("/")[sz-1];
            vallab = the_doc.createElement(null,"vallab");
            vallab.setName("vallab");
            vallab.setAttribute(null,"name",makeStataVarName(the_vallab));
            */
            question_label = q.getLabelInnerText();
            question_tid = q.getTextID();

            /*
            // Tokenise "id" with '/' and obtain unique key of translation in iTextMap
            the_id = question_tid!=null ? question_tid.split("/") : null;
            the_key = null;
            if(the_id!=null && the_id.length>=2){
              the_key = the_id[the_id.length-2] + "/" + the_id[the_id.length-1];
            }
            question_tid = question_tid!=null ? iTextMap.get(the_key) : question_tid;

            //question_tid = question_tid!=null ? iTextMap.get(question_tid) : question_tid;
            question_label = question_label==null && question_tid!=null ? question_tid : (question_label==null ? "" : question_label);
            */

            // ALT: to obtain unique key of translation in iTextMap
            the_key = question_tid!=null ? question_tid.replace(":label","") : null;
            question_tid = question_tid!=null ? iTextMap.get(the_key) : question_tid;
            question_label = question_label==null && question_tid!=null ? question_tid : (question_label==null ? "" : question_label);

            variable_labelsMap.putIfAbsent(the_vallab,question_label);
            if(choices != null){
              for(Object c : choices){
                ch = (SelectChoice) c;
                val = ch.getValue();
                txt = ch.getLabelInnerText();
                tid = ch.getTextID();

                /*
                // Tokenise "id" with '/' and obtain unique key of translation in iTextMap
                the_id = tid!=null ? tid.split("/") : null;
                the_key = null;
                if(the_id!=null && the_id.length>=2){
                  the_key = the_id[the_id.length-2] + "/" + the_id[the_id.length-1];
                }
                the_tid = tid!=null ? iTextMap.get(the_key) : tid;

                //the_tid = tid!=null ? iTextMap.get(tid) : tid;
                // Temp: need to check why iTextMap.get(tid) returns null for tid
                tid = the_tid!=null ? the_tid : tid;
                txt = txt == null && tid !=null ? tid : (txt == null ? "" : txt);
                */

                // ALT: to obtain unique key of translation in iTextMap

                // ALT: to obtain unique key of translation in iTextMap
                the_key = tid!=null ? tid.replace(":label","") : null;
                the_tid = tid!=null ? iTextMap.get(the_key) : tid;

                //the_tid = tid!=null ? iTextMap.get(tid) : tid;
                // Temp: need to check why iTextMap.get(tid) returns null for tid
                tid = the_tid!=null ? the_tid : tid;
                txt = txt == null && tid !=null ? tid : (txt == null ? "" : txt);

                /*
                Element label = the_doc.createElement(null,"label");
                label.setAttribute(null,"value", ch.getValue());
                label.addChild(Node.TEXT,txt);
                vallab.addChild(Node.ELEMENT,label);
                */

                // ALT
                // <label>...
                b.append("<label").append(" ")
                        .append("value=").append("\"").append(ch.getValue()).append("\"")
                        .append(">")
                        .append(txt)
                        .append("</label>");
              }

              /*
              Element value_labels = the_doc.getElement(null,"dta").getElement(null,"value_labels");
              value_labels.addChild(Node.ELEMENT,vallab);
              */

              /*
              //<vallab>...
              String stataVarName = makeStataVarName(the_vallab);
              if(!allVallabsMap.containsKey(stataVarName)){
                allVallabs.append("<vallab").append(" ")
                        .append("name=").append("\"").append(stataVarName).append("\"")
                        .append(">")
                        .append(b.toString())
                        .append("</vallab>");
                //allVallabsMap.put(stataVarName,the_vallab);
                //allVallabsMap.put(the_vallab,stataVarName);
                allVallabsMap.put(the_vallab,b.toString());
              }
              */
              allVallabsMap.put(the_vallab,b.toString());

              // clear
              b.delete(0,b.length());

            }
          }else{
            getQuestions(item, the_doc);
          }
        }
      }
    }catch (IllegalAccessException | InvocationTargetException | SecurityException a){
      a.printStackTrace();
    }
    return true;
  }

  private Map<String,String> processItext(){
    HashMap<String,String> iTextMap = new HashMap<String,String>();
    // Get ODK form model parse tree
    Document f = null;
    try {
      f = XmlManipulationUtils.parseXml(briefcaseLfd.getFormDefinitionFile());
    }catch (FileSystemException fe){
      fe.printStackTrace();
    }catch (ParsingException p){
      p.printStackTrace();
    }
    /*XPath xPath = XPathFactory.newInstance().newXPath();
    try {
      XPathExpression expr = xPath.compile("//translation[@default=\"true()\"]");
    }catch (XPathExpressionException ex){
      ex.printStackTrace();
    }*/


    try {
      Element itext = f.getElement(null, "html").getElement(null, "head").getElement(null, "model").getElement(null, "itext");
      String theLang = null, the_text = null, the_key = null;
      String[] the_id = null;
      String the_idStr = null;
      Boolean isFirst = true;
      int ct = itext.getChildCount();
      int ct_el2 = 0, ct_tr_el = 0;
      Element el2 = null, tr_el = null, value_el = null;
      Object el = null, tr = null, value = null;
      // First see if default language is defined. If yes use that for translationt. Else pick 1st language
      String def = "";
      int start = 0;
      int end=ct;
      // For now we loop through to see if there are any translations, and if any is default translation
      // Need to use xpaths later instead of looping to get start and end
      for(int i=0;i<ct;i++){
        el = itext.getChild(i);
        if (el instanceof Element) {
          el2 = (Element) el;
          theLang = el2.getAttributeValue(null, "lang");
          def = el2.getAttributeValue(null, "default");
          if(def!=null){
            // default language found. Get its index
            start = i;
            end=i+1;
          }
        }
        }
      // Go through <translation> nodes.
      for (int i = start; i < end; i++) {
        el = itext.getChild(i);
        if (el instanceof Element) {
          el2 = (Element) el;
          // We use the default translation if it is available. If not then we use the first
          // that is found. Can improve by using a param to choose choose translation
          if (!el2.getAttributeValue(null, "lang").equals(theLang) && !isFirst) {
            // move onto next tag if this is a translation for a new language
            continue;
          }
          theLang = el2.getAttributeValue(null, "lang");
          isFirst = false;
          ct_el2 = el2.getChildCount();
          // find <text> nodes
          for (int j = 0; j < ct_el2; j++) {
            tr = el2.getChild(j);
            if (tr instanceof Element) {
              tr_el = (Element) tr;
              ct_tr_el = tr_el.getChildCount();
              // cycle through <value> nodes
              for (int k = 0; k < ct_tr_el; k++) {
                value = tr_el.getChild(k);
                if (value instanceof Element) {
                  value_el = (Element) value;
                  // value_el.getText(0)
                  the_text = XFormParser.getXMLText(value_el,true).replaceAll("\\<[^>]*>","");
                  /*
                  // Tokenise "id" with '/' and create unique using the last two entries to save memory
                  //the_id = tr_el.getAttributeValue(null, "id").split("/");
                  the_key = null;
                  if(the_id!=null && the_id.length>=2){
                    the_key = the_id[the_id.length-2] + "/" + the_id[the_id.length-1];
                    iTextMap.put(the_key, the_text);
                  }
                  if(the_id!=null && the_id.length<2){
                    // we have an unexpected format for "id"
                    log.info("Unexpected format for \"id\" of "+the_id.toString());
                  }
                  */
                  the_key = tr_el.getAttributeValue(null, "id").replace(":label","");
                  if(the_key!=null && the_key.length()>0){
                    iTextMap.put(the_key, the_text);
                  }
                }
              }
            }
          }
        }
      }
    }catch (Exception e){
      e.printStackTrace();
      // Problem
      return null;
    }

    return iTextMap;
  }

  private boolean processDtaDescriptors(Document the_doc){
    log.info("Now in processDtaDescriptors");
    // <dta> related
    Element dta = null;
    Element header = null;
    try {
      dta = the_doc.getElement(null, "dta");
      header = dta.getElement(null, "header");
    }catch (Exception e){
      // We have a problem with the document tree. Should not continue
      e.printStackTrace();
      log.info("<typelist> and or <descriptors> not yet created");
      return false;
    }

    //Get observation values for this document
    // Get varsMap
    Map<String,String> varsMap = docVarsMap.get(the_doc);
    //Map<String,Integer> varsMap = docVarsMap.get(the_doc);

    // Update nvar
    try {
      nvar = varsMap.size();
      Element el_nvar = header.getElement(null,"nvar");
      el_nvar.removeChild(0);
      el_nvar.addChild(0,Node.TEXT,Integer.toString(nvar));
      Element el_time_stamp = header.getElement(null,"time_stamp");
      el_time_stamp.addChild(0,Node.TEXT,time_stamp);
    }catch (Exception e){
      e.printStackTrace();
    }

    // Update number of observations
    try {
      Element data = dta.getElement(null,"data");
      Element el_nobs = header.getElement(null, "nobs");
      el_nobs.removeChild(0);
      nobs = data.getChildCount();
      el_nobs.addChild(Node.TEXT, Integer.toString(nobs));
    }catch (Exception e){
      e.printStackTrace();
    }
    log.info("Finished with processDtaDescriptors");

    return true;
  }

  private boolean processDtaData(Document the_doc, OutputStreamWriter w){
    Element data = the_doc.getElement(null,"dta").getElement(null,"data");

    try {
      // Begin <data> contents for this document
      w.append("<data>");

      int ct = data.getChildCount();
      for (int i = 0; i < ct; i++) {
        Element obs = (Element) data.getChild(i);
        // Get the list of <v>s in valsMapBufs for this observation
        List<StringBuilder> vals = valsMapBufs.get(obs);
        // Write <v> strings to associated observation <o>
        w.append("<o name=\"").append(obs.getAttributeValue(null,"name")).append("\">");

        for (StringBuilder val : vals) {
          String s = val.toString();
          w.append(val.toString());
        }
        w.append("</o>");
      }
      // Close <data> section for this document
      w.append("</data>");
    }catch (IOException e){
      e.printStackTrace();
      log.info("Could not append <o> observations data to writer");
    }

    return true;
  }

  private Document setupDta(){
    Document the_doc = new Document();
    //Create <dta> tree and sub-trees first
    Element dta = the_doc.createElement(null,"dta");
    dta.setName("dta");
    // <header>
    Element header = the_doc.createElement(null,"child");
    header.setName("header");
    dta.addChild(0,Node.ELEMENT,header);
    // Add <header> child elements
    Element e = the_doc.createElement(null,"child");
    // <ds_format>
    e.setName("ds_format");
    //Using Dta version 113 for Xml for now because even Stata v13 uses the same for xml out
    e.addChild(0,Node.TEXT,ds_format);
    header.addChild(0,Node.ELEMENT,e);
    // <byteorder>
    e = the_doc.createElement(null,"child");
    e.setName("byteorder");
    e.addChild(0,Node.TEXT,"LOHI");
    header.addChild(1,Node.ELEMENT,e);
    // <nvar>
    e = the_doc.createElement(null,"child");
    e.setName("nvar");
    e.addChild(0,Node.TEXT,Integer.toString(nvar));
    header.addChild(2,Node.ELEMENT,e);
    // <nobs>
    e = the_doc.createElement(null,"child");
    e.setName("nobs");
    e.addChild(0,Node.TEXT,Integer.toString(nobs));
    header.addChild(3,Node.ELEMENT,e);
    // <data_label>
    e = the_doc.createElement(null,"child");
    e.setName("data_label");
    e.addChild(0,Node.TEXT,data_label);
    header.addChild(4,Node.ELEMENT,e);
    // <time_stamp>
    e = the_doc.createElement(null,"child");
    e.setName("time_stamp");
    header.addChild(5,Node.ELEMENT,e);

    // <descriptors>.
    Element descriptors = the_doc.createElement(null,"descriptors");
    descriptors.setName("descriptors");
    dta.addChild(1,Node.ELEMENT,descriptors);
    descriptors_idx = 1;

    // <descriptors> direct child elements (typelist,varlist,srtlist,fmtlist,lbllist)
    Element typelist = the_doc.createElement(null,"typelist");
    Element varlist = the_doc.createElement(null,"varlist");
    Element srtlist = the_doc.createElement(null,"srtlist");
    Element fmtlist = the_doc.createElement(null,"fmtlist");
    Element lbllist = the_doc.createElement(null,"lbllist");
    descriptors.addChild(0,Node.ELEMENT,typelist);
    descriptors.addChild(1,Node.ELEMENT,varlist);
    descriptors.addChild(2,Node.ELEMENT,srtlist);
    descriptors.addChild(3,Node.ELEMENT,fmtlist);
    descriptors.addChild(4,Node.ELEMENT,lbllist);

    // <variable_labels>
    Element variable_labels = the_doc.createElement(null,"variable_labels");
    variable_labels.setName("variable_labels");
    dta.addChild(2,Node.ELEMENT,variable_labels);
    variable_labels_idx = 2;
    // <data>
    Element data = the_doc.createElement(null,"data");
    data.setName("data");
    dta.addChild(3,Node.ELEMENT,data);
    data_idx = 3;
    // <value_labels>
    Element value_labels = the_doc.createElement(null,"value_labels");
    value_labels.setName("value_labels");
    dta.addChild(4,Node.ELEMENT,value_labels);
    value_labels_idx = 4;
    fmtlistMap = new HashMap<String, String>();
    typelistMap = new HashMap<String, String>();
    variable_labelsMap = new HashMap<String, String >();

    // Add <dta> to document object
    the_doc.addChild(0,Node.ELEMENT,dta);
    dta_idx = 0;

    // Add <label> child elements for <vallab> parent tags
    value_labels_all = value_labels;

    return the_doc;
  }

  private String getFullName(AbstractTreeElement e, TreeElement group) {
    List<String> names = new ArrayList<String>();
    AbstractTreeElement oe = e;
    while (e != null && e != group) {
      names.add(e.getName());
      e = e.getParent();
    }
    StringBuilder b = new StringBuilder();
    Collections.reverse(names);
    boolean first = true;
    for (String s : names) {
      if (!first) {
        b.append("-");
      }
      first = false;
      b.append(s);
    }

    return b.toString();
  }

  private String getFullVarName(AbstractTreeElement oe, TreeElement group){
    String s=null;
    // Keep group name to make it easier to find translation text by xpath
    //s = oe.getRef().toString().replaceAll("\\[\\p{Digit}+\\]","");
    s = oe.getRef().toString().replaceAll("\\[(.*?)\\]","");
    return s;
  }

  private String makeStataVarName(String s){

    String tmp2 = null;
    String result = null;
    boolean stop = false;
    // index into names variable starting with furthest parent/ancestor
    int idx = 1;
    String[] names = s.split("/");
    StringBuilder b = new StringBuilder();
    List<String> names2 = new ArrayList<String>();
    int strlen = 0;
    if(useShortVarName && false){
      // variable name is the leaf / last entry in array
      result = names[names.length-idx];
    }else {
      // To build the variable name, we start from the leaf (variable) and
      // ascend the tree of ancestors until the length of the variable name
      // exceeds maxVarNameLength
      // The final variable name should still be unique and will allow Stata to create
      // Note: maxVarNameLength is 3 bytes shorter than max 32 bytes so that
      // Stata can still create temporary variables with them up to 999 temp
      // variables per (because of 3 bytes headroom) if need be

      if(names.length-idx>=0){
        try {
          strlen = names[names.length-idx].length();
          while(!stop && strlen < maxVarNameLength && idx < names.length){
            names2.add(names[names.length-idx]);
            idx++;
            strlen += names[names.length-idx].length();
          }
        }catch (IndexOutOfBoundsException ex){
          ex.printStackTrace();
          log.info("problem?");
          stop=true;
        }
      }


      Collections.reverse(names2);
      boolean first = true;
      for (String str : names2) {
        if (!first) {
          b.append("-");
        }
        first = false;
        b.append(str);
      }
      if(b.length()==0){
        // s has no '/' or max string is shorter than maxVarNameLength
        b.append(s);
      }

      // 1. Remove leading underscore _ or _(\d+) if found, and replace all slashes and hyphens with underscores
      // 2. Replacing hypens with underscores since Stata does not seem to like mixed usage of the two for variable names
      result = b.toString().replaceAll("/","_").replaceAll("-","_").replaceAll("__", "_").replaceFirst("^(_|\\p{Digit}+)","");
    }

    return result;

  }

  private Element findElement(Element submissionElement, String name) {
    if ( submissionElement == null ) {
      return null;
    }
    int maxChildren = submissionElement.getChildCount();
    for (int i = 0; i < maxChildren; i++) {
      if (submissionElement.getType(i) == Node.ELEMENT) {
        Element e = submissionElement.getElement(i);
        if (name.equals(e.getName())) {
          return e;
        }
      }
    }
    return null;
  }

  private List<Element> findElementList(Element submissionElement, String name) {
    List<Element> ecl = new ArrayList<Element>();
    int maxChildren = submissionElement.getChildCount();
    for (int i = 0; i < maxChildren; i++) {
      if (submissionElement.getType(i) == Node.ELEMENT) {
        Element e = submissionElement.getElement(i);
        if (name.equals(e.getName())) {
          ecl.add(e);
        }
      }
    }
    return ecl;
  }

  private String getSubmissionValue(EncryptionInformation ei, TreeElement model, Element element) {
    // could not find element, return null
    if (element == null) {
      return null;
    }

    StringBuilder b = new StringBuilder();

    int maxChildren = element.getChildCount();
    for (int i = 0; i < maxChildren; i++) {
      if (element.getType(i) == Node.TEXT) {
        b.append(element.getText(i));
      }
    }
    String rawElement = b.toString();

    // Field-level encryption support -- experimental
    if ( JavaRosaParserWrapper.isEncryptedField(model) ) {

      InputStreamReader isr = null;
      try {
        Cipher c = ei.getCipher("field:" + model.getName(), model.getName());

        isr = new InputStreamReader(new CipherInputStream(
                  new ByteArrayInputStream(Base64.decodeBase64(rawElement)), c),"UTF-8");

        b.setLength(0);
        int ch;
        while ( (ch = isr.read()) != -1 ) {
          char theChar = (char) ch;
          b.append(theChar);
        }
        return b.toString();

      } catch (IOException e) {
        e.printStackTrace();
        log.debug(" element name: " + model.getName() + " exception: " + e);
      } catch (InvalidKeyException e) {
        e.printStackTrace();
        log.debug(" element name: " + model.getName() + " exception: " + e);
      } catch (InvalidAlgorithmParameterException e) {
        e.printStackTrace();
        log.debug(" element name: " + model.getName() + " exception: " + e);
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
        log.debug(" element name: " + model.getName() + " exception: " + e);
      } catch (NoSuchPaddingException e) {
        e.printStackTrace();
        log.debug(" element name: " + model.getName() + " exception: " + e);
      } finally {
        if (isr != null) {
          try {
            isr.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
    return rawElement;
  }

  private boolean emitSubmissionDta(OutputStreamWriter osw, EncryptionInformation ei,
      Element submissionElement,
      TreeElement primarySet, TreeElement treeElement, boolean first, String uniquePath,
      File instanceDir, int trueOrdinal, Document the_doc) throws IOException {
    // OK -- group with at least one element -- assume no value...
    // TreeElement list has the begin and end tags for the nested groups.
    // Swallow the end tag by looking to see if the prior and current
    // field names are the same.
    TreeElement prior = null;
    Element obs = null;
    Integer var_ct = 0;
    int r_ct=0;

    try{
      // ct is the most recently added observation in the <data> sub-tree
      int ct = the_doc.getElement(null,"dta").getElement(null,"data").getChildCount() - 1;
      obs = the_doc.getElement(null,"dta").getElement(null,"data").getElement(ct);
    }catch(Exception e){
      // We have a problem with the document tree. Should not continue
      e.printStackTrace();
      log.info("<dta> and or <data> not yet created");
      return false;
    }
    // Try to get variables map for this document
    Map<String,String> varsMap = null;
    //Map<String,Integer> varsMap = null;
    try{
      varsMap = docVarsMap.get(the_doc);
    }catch (NullPointerException x){
      x.printStackTrace();
      log.info("Could not get variables map for this submission/tree element");
    }

    //StringBuffer s = new StringBuffer();
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < treeElement.getNumChildren(); ++i) {
      TreeElement current = (TreeElement) treeElement.getChildAt(i);
      if ((prior != null) && (prior.getName().equals(current.getName()))) {
        // it is the end-group tag... seems to happen with two adjacent repeat
        // groups
        log.info("repeating tag at " + i + " skipping " + current.getName());
        prior = current;
      } else {
        Element ec = findElement(submissionElement, current.getName());
        String the_val="";
        var_ct = 0;
        switch (current.getDataType()) {
        case org.javarosa.core.model.Constants.DATATYPE_TEXT:/**
           * Text question
           * type.
           */
        case org.javarosa.core.model.Constants.DATATYPE_INTEGER:/**
           * Numeric
           * question type. These are numbers without decimal points
           */
        case org.javarosa.core.model.Constants.DATATYPE_DECIMAL:/**
           * Decimal
           * question type. These are numbers with decimals
           */
        case org.javarosa.core.model.Constants.DATATYPE_CHOICE:/**
           * This is a
           * question with alist of options where not more than one option can
           * be selected at a time.
           */
        case org.javarosa.core.model.Constants.DATATYPE_CHOICE_LIST:/**
           * This is a
           * question with alist of options where more than one option can be
           * selected at a time.
           */
        case org.javarosa.core.model.Constants.DATATYPE_BOOLEAN:/**
           * Question with
           * true and false answers.
           */
        case org.javarosa.core.model.Constants.DATATYPE_BARCODE:/**
           * Question with
           * barcode string answer.
           */
        default:
        case org.javarosa.core.model.Constants.DATATYPE_UNSUPPORTED:
          if (ec == null) {

          } else {
            the_val = getSubmissionValue(ei,current,ec);

          }
          first = false;
          break;
        case org.javarosa.core.model.Constants.DATATYPE_DATE:
          /**
           * Date question type. This has only date component without time.
           */
          if (ec == null) {

          } else {
            String value = getSubmissionValue(ei,current,ec);
            if (value == null || value.length() == 0) {

            } else {
              Date date = WebUtils.parseDate(value);
              DateFormat formatter = DateFormat.getDateInstance();

              the_val = formatter.format(date);
            }
          }
          first = false;
          break;
        case org.javarosa.core.model.Constants.DATATYPE_TIME:
          /**
           * Time question type. This has only time element without date
           */
          if (ec == null) {

          } else {
            String value = getSubmissionValue(ei,current,ec);
            if (value == null || value.length() == 0) {

            } else {
              Date date = WebUtils.parseDate(value);
              DateFormat formatter = DateFormat.getTimeInstance();

              the_val = formatter.format(date);
            }
          }
          first = false;
          break;
        case org.javarosa.core.model.Constants.DATATYPE_DATE_TIME:
          /**
           * Date and Time question type. This has both the date and time
           * components
           */
          if (ec == null) {

          } else {
            String value = getSubmissionValue(ei,current,ec);
            if (value == null || value.length() == 0) {

            } else {
              Date date = WebUtils.parseDate(value);
              DateFormat formatter = DateFormat.getDateTimeInstance();

              the_val = formatter.format(date);
            }
          }
          first = false;
          break;
        case org.javarosa.core.model.Constants.DATATYPE_GEOPOINT:
          /**
           * Question with location answer.
           */
          String compositeValue = (ec == null) ? null : getSubmissionValue(ei,current,ec);
          compositeValue = (compositeValue == null) ? null : compositeValue.trim();
          the_val = compositeValue;

          // emit separate lat, long, alt, acc columns...
          if (compositeValue == null || compositeValue.length() == 0) {
            for (int count = 0; count < 4; ++count) {

              first = false;
            }
          } else {
            String[] values = compositeValue.split(" ");
            for (String value : values) {

              first = false;
            }
            for (int count = values.length; count < 4; ++count) {

              first = false;
            }
          }
          break;
        case org.javarosa.core.model.Constants.DATATYPE_BINARY:
          /**
           * Question with external binary answer e.g Media files
           */
          String binaryFilename = getSubmissionValue(ei,current,ec);
          if (binaryFilename == null || binaryFilename.length() == 0) {

            first = false;
          } else {
            if (exportMedia) {
               int dotIndex = binaryFilename.lastIndexOf(".");
               String namePart = (dotIndex == -1) ? binaryFilename : binaryFilename.substring(0,
                   dotIndex);
               String extPart = (dotIndex == -1) ? "" : binaryFilename.substring(dotIndex);

               File binaryFile = new File(instanceDir, binaryFilename);
               String destBinaryFilename = binaryFilename;
               int version = 1;
               File destFile = new File(outputMediaDir, destBinaryFilename);
               while (destFile.exists()) {
                 destBinaryFilename = namePart + "-" + (++version) + extPart;
                 destFile = new File(outputMediaDir, destBinaryFilename);
               }
               if ( binaryFile.exists() ) {
                 FileUtils.copyFile(binaryFile, destFile);
               }

               the_val = MEDIA_DIR + File.separator + destFile.getName();
            } else {

                the_val = binaryFilename;
            }

            first = false;
          }
          break;
        case org.javarosa.core.model.Constants.DATATYPE_NULL: /*
                                                               * for nodes that
                                                               * have no data,
                                                               * or data type
                                                               * otherwise
                                                               * unknown
                                                               */
          if (current.isRepeatable()) {
            if (prior == null || !current.getName().equals(prior.getName())) {
              // repeatable group...
              if (ec == null) {

                first = false;
              } else {
                String uniqueGroupPath = uniquePath + "/" + getFullName(current, primarySet);

                // DVB: Should we take this value?
                the_val = uniqueGroupPath;
                first = false;
                // first time processing this repeat group (ignore templates)
                List<Element> ecl = findElementList(submissionElement, current.getName());
                  emitRepeatingGroupDta(ei, ecl, current, uniquePath,
                                                    uniqueGroupPath, instanceDir);

              }
            }
          } else if (current.getNumChildren() == 0 && current != briefcaseLfd.getSubmissionElement()) {
            // assume fields that don't have children are string fields.
            if (ec == null) {

              first = false;
            } else {

              first = false;
              the_val = getSubmissionValue(ei,current,ec);
            }
          } else {
            /* one or more children -- this is a non-repeating group */
            first = emitSubmissionDta(osw, ei, ec, primarySet, current, first, uniquePath, instanceDir, trueOrdinal, the_doc);
          }
          break;
        }

        // Add the value tag <v> to the observations
        b.append(getFullVarName(current, primarySet));
        if(current.getNumChildren() == 0){
          varsMap.put(b.toString(),current.getName());

          List<StringBuilder> v2 = valsMapBufs.get(obs);
          if(v2==null){
            v2 = new ArrayList<StringBuilder>();
          }

          StringBuilder s = new StringBuilder();
          String new_varname = null;

          if(useShortVarName){
            new_varname = repeatVarsMap.get(b.toString());
            // Create special short names for special repeat variables which are used for
            // multiple bind nodesets update counter in repeatMap to account for remaining
            // counts for special short names of special repeats
            if(new_varname!=null){
              r_ct = repeatMap.get(new_varname);
              new_varname = new_varname+"_00"+r_ct;
              r_ct--;
              repeatMap.put(repeatVarsMap.get(b.toString()),r_ct);
              // Update entry in repeatVarsMap with its new name so we can reuse it
              // to update the associated <v> entries that use the longname etc.
              //repeatVarsMap.put(b.toString(),new_varname);
            }else{
              // Get original variable name. This is the same as in the form
              new_varname = varsMap.get(b.toString());
            }
          }else{
            // keep long name
            new_varname = b.toString();
          }

          s.append("<v").append(" ")
                  //.append("varname=").append("\"").append(current.getName()).append("\"")
                  //.append("varname=").append("\"").append(makeStataVarName(b.toString())).append("\"")
                  .append("varname=").append("\"").append(makeStataVarName(new_varname)).append("\"")
                  .append(">")
                  .append(the_val)
                  .append("</v>");

          v2.add(s);
        }

        // Update fmt and type for Choice numeric-type values if not blank/empty string
        // Only doing Choice because we are losing data for mixed numeric/non-numeric data in a
        // variable, especially if the ODK type isn't specified.
        if(current.getDataType() == org.javarosa.core.model.Constants.DATATYPE_CHOICE){
          try{
            Integer val2 = Integer.parseInt(the_val);
            if(val2 instanceof Integer && Math.abs(val2)>=0){
              // Treat all Integers as Stata double in case values are mixed for variable
              //typelistMap.put(current.getName(),"double");
              //fmtlistMap.put(current.getName(),"%15.0g");
              typelistMap.put(b.toString(),"double");
              fmtlistMap.put(b.toString(),"%15.0g");
            }
          }catch (Exception e){
            // not an Integer
            try{
              Double vald = Double.parseDouble(the_val);
              Float valf = Float.parseFloat(the_val);
              if((vald instanceof Double || valf instanceof Float) && (Math.abs(vald)>=0 || Math.abs(valf.floatValue())>=0)){
                //typelistMap.put(current.getName(),"double");
                //fmtlistMap.put(current.getName(),"%15.0g");
                typelistMap.put(b.toString(),"double");
                fmtlistMap.put(b.toString(),"%15.0g");
              }
            }catch (Exception f){
              // If value is a choice type and it isn't Double or Float treat it as string. Need to review this later
            }
          }
        }
        // clear
        b.delete(0,b.length());


        prior = current;
        // Cleanup
        current = null;
        ec = null;
      }
    }

    return first;
  }

  private void emitRepeatingGroupDta(EncryptionInformation ei, List<Element> groupElementList, TreeElement group,
      String uniqueParentPath, String uniqueGroupPath, File instanceDir)
      throws IOException {
    OutputStreamWriter osw = fileMap.get(group);
    Document the_doc = docMap.get(group);
    int trueOrdinal = 1;
    for ( Element groupElement : groupElementList ) {
      String uniqueGroupInstancePath = uniqueGroupPath + "[" + trueOrdinal + "]";
      boolean first = true;
      Element obs = the_doc.createElement(null,"o");
      obs.setName("o");
      obs.setAttribute(null,"name",uniqueGroupInstancePath);
      Element data = the_doc.getElement(null,"dta").getElement(null,"data");
      data.addChild(Node.ELEMENT, obs);

      // ALT: Prep list to store <v> values for this <obs> observation
      List<StringBuilder> listVals = new ArrayList<>();
      valsMapBufs.put(obs,listVals);

      first = emitSubmissionDta(osw, ei, groupElement, group, group, first, uniqueGroupInstancePath, instanceDir, trueOrdinal, the_doc);

      // Add PARENT_KEY, KEY values
      StringBuilder s = new StringBuilder();
      s.append("<v").append(" ")
              .append("varname=").append("\"").append("PARENT_KEY").append("\"")
              .append(">")
              .append(uniqueParentPath)
              .append("</v>");

      s.append("<v").append(" ")
              .append("varname=").append("\"").append("KEY").append("\"")
              .append(">")
              .append(uniqueGroupInstancePath)
              .append("</v>");

      listVals.add(s);

      ++trueOrdinal;
    }
  }

  private boolean emitDtaHeaders(OutputStreamWriter osw, TreeElement primarySet,
      TreeElement treeElement, boolean first, Document the_doc) throws IOException {
    // OK -- group with at least one element -- assume no value...
    // TreeElement list has the begin and end tags for the nested groups.
    // Swallow the end tag by looking to see if the prior and current
    // field names are the same.
    TreeElement prior = null;
    StringBuilder b = new StringBuilder();

    for (int i = 0; i < treeElement.getNumChildren(); ++i) {
      TreeElement current = (TreeElement) treeElement.getChildAt(i);
      if ((prior != null) && (prior.getName().equals(current.getName()))) {
        // it is the end-group tag... seems to happen with two adjacent repeat
        // groups
        log.info("repeating tag at " + i + " skipping " + current.getName());
        prior = current;
      } else {
        String the_type = "";
        String the_fmt = "";
        String the_val="";
        String fullname = null;

        switch (current.getDataType()) {
        case org.javarosa.core.model.Constants.DATATYPE_TEXT:/**
           * Text question
           * type.
           */
          the_type = the_type=="" ? "str244" : the_type;
          the_fmt = the_fmt=="" ? "%13s" : the_fmt;
        case org.javarosa.core.model.Constants.DATATYPE_INTEGER:/**
           * Numeric
           * question type. These are numbers without decimal points
           */
          the_type = the_type=="" ? "double" : the_type;
          the_fmt = the_fmt=="" ? "%15.0g" : the_fmt;
        case org.javarosa.core.model.Constants.DATATYPE_DECIMAL:/**
           * Decimal
           * question type. These are numbers with decimals
           */
          the_type = the_type=="" ? "double" : the_type;
          the_fmt = the_fmt=="" ? "%15.0g" : the_fmt;
        case org.javarosa.core.model.Constants.DATATYPE_DATE:/**
           * Date question
           * type. This has only date component without time.
           */
          the_type = the_type=="" ? "str244" : the_type;
          the_fmt = the_fmt=="" ? "%20s" : the_fmt;
        case org.javarosa.core.model.Constants.DATATYPE_TIME:/**
           * Time question
           * type. This has only time element without date
           */
          the_type = the_type=="" ? "str244" : the_type;
          the_fmt = the_fmt=="" ? "%20s" : the_fmt;
        case org.javarosa.core.model.Constants.DATATYPE_DATE_TIME:/**
           * Date and
           * Time question type. This has both the date and time components
           */
          the_type = the_type=="" ? "str244" : the_type;
          the_fmt = the_fmt=="" ? "%20s" : the_fmt;
        case org.javarosa.core.model.Constants.DATATYPE_CHOICE:/**
           * This is a
           * question with alist of options where not more than one option can
           * be selected at a time.
           */
          the_type = the_type=="" ? "double" : the_type;
          the_fmt = the_fmt=="" ? "%30.0g" : the_fmt;
          case org.javarosa.core.model.Constants.DATATYPE_CHOICE_LIST:/**
           * This is a
           * question with alist of options where more than one option can be
           * selected at a time.
           */
            the_type = the_type=="" ? "str244" : the_type;
            the_fmt = the_fmt=="" ? "%13s" : the_fmt;
          case org.javarosa.core.model.Constants.DATATYPE_BOOLEAN:/**
           * Question with
           * true and false answers.
           */
            the_type = the_type=="" ? "byte" : the_type;
            the_fmt = the_fmt=="" ? "%10.0g" : the_fmt;
        case org.javarosa.core.model.Constants.DATATYPE_BARCODE:/**
           * Question with
           * barcode string answer.
           */
          the_type = the_type=="" ? "str244" : the_type;
          the_fmt = the_fmt=="" ? "%13s" : the_fmt;
        case org.javarosa.core.model.Constants.DATATYPE_BINARY:/**
           * Question with
           * external binary answer.
           */
          // What should we do here? Ignore or keep? strL
          the_type = the_type=="" ? "str244" : the_type;
          the_fmt = the_fmt=="" ? "%9s" : the_fmt;
        default:
        case org.javarosa.core.model.Constants.DATATYPE_UNSUPPORTED:

          first = false;
          the_type = the_type=="" ? "str244" : the_type;
          the_fmt = the_fmt=="" ? "%13s" : the_fmt;
          b.append(getFullVarName(current, primarySet));
          break;
        case org.javarosa.core.model.Constants.DATATYPE_GEOPOINT:
          /**
           * Question with location answer.
           */




          first = false;
          the_type = the_type=="" ? "str244" : the_type;
          the_fmt = the_fmt=="" ? "%13s" : the_fmt;
          //fullname = getFullVarName(current, primarySet);
          b.append(getFullVarName(current, primarySet));
          break;
        case org.javarosa.core.model.Constants.DATATYPE_NULL: /*
                                                               * for nodes that
                                                               * have no data,
                                                               * or data type
                                                               * otherwise
                                                               * unknown
                                                               */
          if (current.isRepeatable()) {
            // repeatable group...

            first = false;
            the_type = the_type=="" ? "str244" : the_type;
            the_fmt = the_fmt=="" ? "%13s" : the_fmt;
            //fullname = "SET_OF_" + getFullVarName(current, primarySet);
            b.append("SET_OF_");
            b.append(getFullVarName(current, primarySet));

            processRepeatingGroupDefinition(current, primarySet, true);

          } else if (current.getNumChildren() == 0 && current != briefcaseLfd.getSubmissionElement()) {
            // assume fields that don't have children are string fields.
            //fullname = getFullVarName(current, primarySet);
            b.append(getFullVarName(current, primarySet));
            first = false;
          } else {
            /* one or more children -- this is a non-repeating group */
            first = emitDtaHeaders(osw, primarySet, current, first, the_doc);
          }
          break;
        }
        /*
        if(current.getNumChildren() == 0 && the_type!=null){
          fmtlistMap.putIfAbsent(current.getName(),the_fmt);
          typelistMap.putIfAbsent(current.getName(),the_type);
        }else if(current.isRepeatable()){

        }
        */
        if(b.length()>0){
          fmtlistMap.putIfAbsent(b.toString(),the_fmt);
          typelistMap.putIfAbsent(b.toString(),the_type);
          Integer ct = repeatMap.get(current.getName());
          if(useShortVarName && ct!=null && !current.getParent().isRepeatable()){
            // the same variable name is associated with multiple bind nodeset entries
            ct++;
            repeatVarsMap.put(b.toString(),current.getName());
          }
          ct = ct==null ? 0 : ct;
          repeatMap.put(current.getName(),ct);
        }
        // clear
        b.delete(0,b.length());

        prior = current;
      }
    }

    return first;
  }

  private void populateRepeatGroupsIntoFileMap(TreeElement primarySet,
      TreeElement treeElement) throws IOException {
    // OK -- group with at least one element -- assume no value...
    // TreeElement list has the begin and end tags for the nested groups.
    // Swallow the end tag by looking to see if the prior and current
    // field names are the same.
    TreeElement prior = null;
    for (int i = 0; i < treeElement.getNumChildren(); ++i) {
      TreeElement current = (TreeElement) treeElement.getChildAt(i);
      if ((prior != null) && (prior.getName().equals(current.getName()))) {
        // it is the end-group tag... seems to happen with two adjacent repeat
        // groups
        log.info("repeating tag at " + i + " skipping " + current.getName());
        prior = current;
      } else {
        switch (current.getDataType()) {
        default:
          break;
        case org.javarosa.core.model.Constants.DATATYPE_NULL:
          /* for nodes that have no data, or data type otherwise unknown */
          if (current.isRepeatable()) {
            processRepeatingGroupDefinition(current, primarySet, false);
          } else if (current.getNumChildren() == 0 && current != briefcaseLfd.getSubmissionElement()) {
            // ignore - string type
          } else {
            /* one or more children -- this is a non-repeating group */
            populateRepeatGroupsIntoFileMap(primarySet, current);
          }
          break;
        }
        prior = current;
      }
    }
  }

  private void processRepeatingGroupDefinition(TreeElement group, TreeElement primarySet, boolean emitDtaHeaders)
      throws IOException {
    String formName = baseFilename + "-" + getFullName(group, primarySet);
    File topLevelDta = new File(outputDir, safeFilename(formName) + ".xml");
    FileOutputStream os = new FileOutputStream(topLevelDta, !overwrite);
    OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");
    fileMap.put(group, osw);
    // Dta related
    Document the_doc = setupDta();
    docMap.put(group,the_doc);

    // Add PARENT_KEY, KEY, SET-OF variables.

    Map<String,String> varsMap = new HashMap<String,String>();
    docVarsMap.put(the_doc,varsMap);
    varsMap.put("PARENT_KEY","PARENT_KEY");
    varsMap.put("KEY","KEY");
    varsMap.put("SET-OF-" + group.getName(),"SET-OF-" + group.getName());


    /*
    Map<String,Integer> varsMap = new HashMap<String,Integer>();
    docVarsMap.put(the_doc,varsMap);
    varsMap.put("PARENT_KEY",1);
    varsMap.put("KEY",1);
    varsMap.put("SET-OF-" + group.getName(),1);
    */

    // Add PARENT_KEY, KEY, SET-OF type and format.
    typelistMap.put("PARENT_KEY","str244");
    fmtlistMap.put("PARENT_KEY","%13s");
    typelistMap.put("KEY","str244");
    fmtlistMap.put("KEY","%13s");
    typelistMap.put("SET-OF-" + group.getName(),"str244");
    fmtlistMap.put("SET-OF-" + group.getName(),"%13s");


    if ( emitDtaHeaders ) {
      boolean first = true;
      first = emitDtaHeaders(osw, group, group, first, the_doc);
    } else {
      populateRepeatGroupsIntoFileMap(group, group);
    }
  }

  private String safeFilename(String name) {
    return name.replaceAll("\\p{Punct}", "_")
            .replace("\\p{Space}", "_").replaceAll("__", "_")
            .replaceAll("[_]*$", "").replaceAll(" ","_");
  }

  private boolean processFormDefinition() {

    TreeElement submission = briefcaseLfd.getSubmissionElement();

    String formName = baseFilename;
    File topLevelDta = new File(outputDir, safeFilename(formName) + ".xml");
    boolean exists = topLevelDta.exists();
    FileOutputStream os;
    try {
      os = new FileOutputStream(topLevelDta, !overwrite);
      OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");
      fileMap.put(submission, osw);
      Document the_doc = setupDta();
      docMap.put(submission,the_doc);
      Map<String,String> varsMap = new HashMap<String,String>();
      //Map<String,Integer> varsMap = new HashMap<String,Integer>();
      docVarsMap.put(the_doc,varsMap);

      fd = briefcaseLfd.getFormDefn();

      // only write headers if overwrite is set, or creating file for the first time
      if (overwrite || !exists) {

          emitDtaHeaders(osw, submission, submission, false, the_doc);

          if ( briefcaseLfd.isFileEncryptedForm() ) {

          }
       } else {
         populateRepeatGroupsIntoFileMap(submission, submission);
       }
      repeatMapTmp.putAll(repeatMap);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      EventBus.publish(new ExportProgressEvent("Unable to create dta file: "
          + topLevelDta.getPath()));
      for (OutputStreamWriter w : fileMap.values()) {
        try {
          w.close();
        } catch (IOException e1) {
          e1.printStackTrace();
        }
      }
      fileMap.clear();
      iTextMap.clear();
      fmtlistMap.clear();
      typelistMap.clear();
      return false;
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      EventBus.publish(new ExportProgressEvent("Unable to create dta file: "
          + topLevelDta.getPath()));
      for (OutputStreamWriter w : fileMap.values()) {
        try {
          w.close();
        } catch (IOException e1) {
          e1.printStackTrace();
        }
      }
      fileMap.clear();
      iTextMap.clear();
      fmtlistMap.clear();
      typelistMap.clear();
      return false;
    } catch (IOException e) {
      e.printStackTrace();
      EventBus.publish(new ExportProgressEvent("Unable to create dta file: "
          + topLevelDta.getPath()));
      for (OutputStreamWriter w : fileMap.values()) {
        try {
          w.close();
        } catch (IOException e1) {
          e1.printStackTrace();
        }
      }
      fileMap.clear();
      iTextMap.clear();
      fmtlistMap.clear();
      typelistMap.clear();
      return false;
    }
    return true;
  }

  private boolean processInstance(File instanceDir) {
    File submission = new File(instanceDir, "submission.xml");
    if (!submission.exists() || !submission.isFile()) {
      EventBus.publish(new ExportProgressEvent("Submission not found for instance directory: "
          + instanceDir.getPath()));
      return false;
    }
    EventBus.publish(new ExportProgressEvent("Processing instance: " + instanceDir.getName()));

    // If we are encrypted, be sure the temporary directory
    // that will hold the unencrypted files is created and empty.
    // If we aren't encrypted, the temporary directory
    // is the same as the instance directory.

    File unEncryptedDir;
    if (briefcaseLfd.isFileEncryptedForm()) {
      // create or clean-up the temp directory that will hold the unencrypted
      // files. Do this in the outputDir so that the briefcase storage location
      // can be a read-only network mount. issue 676.
      unEncryptedDir = new File(outputDir, ".temp");

      if (unEncryptedDir.exists()) {
        // silently delete it...
        try {
          FileUtils.deleteDirectory(unEncryptedDir);
        } catch (IOException e) {
          e.printStackTrace();
          EventBus.publish(new ExportProgressEvent("Unable to delete stale temp directory: "
              + unEncryptedDir.getAbsolutePath()));
          return false;
        }
      }

      if (!unEncryptedDir.mkdirs()) {
        EventBus.publish(new ExportProgressEvent("Unable to create temp directory: "
            + unEncryptedDir.getAbsolutePath()));
        return false;
      }
    } else {
      unEncryptedDir = instanceDir;
    }

    // parse the xml document (this is the manifest if encrypted)...
    Document doc;
    boolean isValidated = false;

    try {
      doc = XmlManipulationUtils.parseXml(submission);
    } catch (ParsingException e) {
      e.printStackTrace();
      EventBus.publish(new ExportProgressEvent("Error parsing submission "
          + instanceDir.getName() + " Cause: " + e.toString()));
      return false;
    } catch (FileSystemException e) {
      e.printStackTrace();
      EventBus.publish(new ExportProgressEvent("Error parsing submission "
          + instanceDir.getName() + " Cause: " + e.toString()));
      return false;
    }

    String submissionDate = null;
    // extract the submissionDate, if present, from the attributes
    // of the root element of the submission or submission manifest (if encrypted).
    submissionDate = doc.getRootElement().getAttributeValue(null, "submissionDate");
    if (submissionDate == null || submissionDate.length() == 0) {
      submissionDate = null;
    } else {
      Date theDate = WebUtils.parseDate(submissionDate);
      DateFormat formatter = DateFormat.getDateTimeInstance();
      submissionDate = formatter.format(theDate);

      // just return true to skip records out of range
      // DVB: Should we support this for dta xml? Since xml file is overwritten,
      // previously processed data in the dta xml will be lost
      // 1. If supported, then we need to read dta xml if it exists and load into doc2
      // then do rest of processing. dta xml should be read before processInstance is called though.
      if (startDate != null && theDate.before(startDate)) {
          log.info("Submission date is before specified, skipping: " + instanceDir.getName());
          return true;
      }
      if (endDate != null && theDate.after(endDate)) {
          log.info("Submission date is after specified, skipping: " + instanceDir.getName());
          return true;
      }
      // don't export records without dates if either date is set
      if ((startDate != null || endDate != null) && submissionDate == null) {
          log.info("No submission date found, skipping: " + instanceDir.getName());
          return true;
      }
    }

    // Beyond this point, we need to have a finally block that
    // will clean up any decrypted files whenever there is any
    // failure.
    try {

      if (briefcaseLfd.isFileEncryptedForm()) {
        // Decrypt the form and all its media files into the
        // unEncryptedDir and validate the contents of all
        // those files.
        // NOTE: this changes the value of 'doc'
        try {
          FileSystemUtils.DecryptOutcome outcome =
            FileSystemUtils.decryptAndValidateSubmission(doc, briefcaseLfd.getPrivateKey(),
              instanceDir, unEncryptedDir);
          doc = outcome.submission;
          isValidated = outcome.isValidated;
        } catch (ParsingException e) {
          e.printStackTrace();
          EventBus.publish(new ExportProgressEvent("Error decrypting submission "
              + instanceDir.getName() + " Cause: " + e.toString()));
          //DVB: For Now just do this repeatedly. We'll update to current style of ExportToCsv.java later
          //update total number of files skipped
          totalFilesSkipped++;
          return false;
        } catch (FileSystemException e) {
          e.printStackTrace();
          EventBus.publish(new ExportProgressEvent("Error decrypting submission "
              + instanceDir.getName() + " Cause: " + e.toString()));
          //DVB: For Now just do this repeatedly. We'll update to current style of ExportToCsv.java later
          //update total number of files skipped
          totalFilesSkipped++;
          return false;
        } catch (CryptoException e) {
          e.printStackTrace();
          EventBus.publish(new ExportProgressEvent("Error decrypting submission "
              + instanceDir.getName() + " Cause: " + e.toString()));//DVB: For Now just do this repeatedly. We'll update to current style of ExportToCsv.java later
          //update total number of files skipped
          totalFilesSkipped++;
          return false;
        }
      }

      String instanceId = null;
      String base64EncryptedFieldKey = null;
      // find an instanceId to use...
      try {
        FormInstanceMetadata sim = XmlManipulationUtils.getFormInstanceMetadata(doc
            .getRootElement());
        instanceId = sim.instanceId;
        base64EncryptedFieldKey = sim.base64EncryptedFieldKey;
      } catch (ParsingException e) {
        e.printStackTrace();
        EventBus.publish(new ExportProgressEvent("Could not extract metadata from submission: "
            + submission.getAbsolutePath() + " Cause: " + e.toString()));
        return false;
      }

      if (instanceId == null || instanceId.length() == 0) {
        // if we have no instanceID, and there isn't any in the file,
        // use the checksum as the id.
        // NOTE: encrypted submissions always have instanceIDs.
        // This is for legacy non-OpenRosa forms.
        long checksum;
        try {
          checksum = FileUtils.checksumCRC32(submission);
        } catch (IOException e1) {
          e1.printStackTrace();
          EventBus.publish(new ExportProgressEvent("Failed during computing of crc: "
              + e1.getMessage()));
          return false;
        }
        instanceId = "crc32:" + Long.toString(checksum);
      }

      if ( terminationFuture.isCancelled() ) {
        EventBus.publish(new ExportProgressEvent("ABORTED"));
        return false;
      }

      EncryptionInformation ei = null;
      if ( base64EncryptedFieldKey != null ) {
        try {
          ei = new EncryptionInformation(base64EncryptedFieldKey, instanceId, briefcaseLfd.getPrivateKey());
        } catch (CryptoException e) {
          e.printStackTrace();
          EventBus.publish(new ExportProgressEvent("Error establishing field decryption for submission "
              + instanceDir.getName() + " Cause: " + e.toString()));
          return false;
        }
      }

      // emit the dta xml record...
      try {
        OutputStreamWriter osw = fileMap.get(briefcaseLfd.getSubmissionElement());
        Document the_doc = docMap.get(briefcaseLfd.getSubmissionElement());
        time_stamp = submissionDate;

        Element obs = the_doc.createElement(null,"o");
        obs.setName("o");
        obs.setAttribute(null,"name",instanceId);
        Element data = the_doc.getElement(null,"dta").getElement(null,"data");
        data.addChild(Node.ELEMENT, obs);

        // ALT: Prep list to store <v> values for this <obs> observation
        List<StringBuilder> listVals = new ArrayList<>();
        valsMapBufs.put(obs,listVals);


        emitSubmissionDta(osw, ei, doc.getRootElement(), briefcaseLfd.getSubmissionElement(),
            briefcaseLfd.getSubmissionElement(), false, instanceId, unEncryptedDir, 0, the_doc);

        if ( briefcaseLfd.isFileEncryptedForm() ) {

          if ( !isValidated ) {
            EventBus.publish(new ExportProgressEvent("Decrypted submission "
                + instanceDir.getName() + " may be missing attachments and could not be validated."));
          }
        }
        //reset
        repeatMap.putAll(repeatMapTmp);

        // Cleanup
        doc = null;
        submission = null;
        obs = null;
        return true;

      } catch (IOException e) {
        e.printStackTrace();
        EventBus.publish(new ExportProgressEvent("Failed writing dta xml: " + e.getMessage()));
        return false;
      }
    } finally {
      if (briefcaseLfd.isFileEncryptedForm()) {
        // destroy the temp directory and its contents...
        try {
          FileUtils.deleteDirectory(unEncryptedDir);
        } catch (IOException e) {
          e.printStackTrace();
          EventBus.publish(new ExportProgressEvent("Unable to remove decrypted files: "
              + e.getMessage()));
          return false;
        }
      }
    }
  }

  @Override
  public FilesSkipped totalFilesSkipped() {
    //Determine if all files where skipped or just some
    //Note that if totalInstances = 0 then no files were skipped
    if (totalInstances == 0 || totalFilesSkipped == 0) {
      return FilesSkipped.NONE;
    }
    if (totalFilesSkipped == totalInstances) {
      return FilesSkipped.ALL;
    } else {
      return FilesSkipped.SOME;
    }
  }

  @Override
  public BriefcaseFormDefinition getFormDefinition() {
    return briefcaseLfd;
  }
}
