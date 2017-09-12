package org.opendatakit.briefcase.util;

import org.opendatakit.briefcase.model.BriefcaseFormDefinition;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

public class FormCache {
    private final File cacheFile;
    private Map<String, String> pathToMd5Map = new HashMap<>();
    private Map<String, BriefcaseFormDefinition> pathToDefinitionMap = new HashMap<>();

    public FormCache(File storagePath) {
        cacheFile = new File(storagePath, "cache.ser");
        if (cacheFile.exists() && cacheFile.canRead()) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(cacheFile))) {
                pathToMd5Map = (Map) objectInputStream.readObject();
                pathToDefinitionMap = (Map) objectInputStream.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                save();
            }
        });
    }

    private void save() {
        if (!cacheFile.exists() || cacheFile.canWrite()) {
            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(cacheFile))) {
                objectOutputStream.writeObject(pathToMd5Map);
                objectOutputStream.writeObject(pathToDefinitionMap);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String getFormFileMd5Hash(String filePath) {
        return pathToMd5Map.get(filePath);
    }

    public void putFormFileMd5Hash(String filePath, String md5Hash) {
        pathToMd5Map.put(filePath, md5Hash);
    }

    public BriefcaseFormDefinition getFormFileFormDefinition(String filePath) {
        if (pathToDefinitionMap == null) {
            pathToDefinitionMap = new HashMap<>();
        }
        return pathToDefinitionMap.get(filePath);
    }

    public void putFormFileFormDefinition(String filePath, BriefcaseFormDefinition definition) {
        pathToDefinitionMap.put(filePath, definition);
    }
}
