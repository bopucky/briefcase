/*
 * Copyright (C) 2018 Nafundi
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

package org.opendatakit.briefcase.export;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Optional;
import org.bouncycastle.openssl.PEMReader;
import org.bushe.swing.event.EventBus;
import org.opendatakit.briefcase.model.BriefcaseFormDefinition;
import org.opendatakit.briefcase.model.ExportFailedEvent;
import org.opendatakit.briefcase.model.ExportSucceededEvent;
import org.opendatakit.briefcase.model.ExportSucceededWithErrorsEvent;
import org.opendatakit.briefcase.model.TerminationFuture;
import org.opendatakit.briefcase.util.ErrorsOr;
import org.opendatakit.briefcase.util.ExportToCsv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// DVB
import org.opendatakit.briefcase.util.ExportToDta;
import org.opendatakit.briefcase.model.ExportType;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import org.opendatakit.briefcase.operations.ExportException;
import java.util.ArrayList;
import java.util.List;

public class ExportAction {
  private static final Logger log = LoggerFactory.getLogger(ExportAction.class);

  private static Optional<PrivateKey> extractPrivateKey(Object o) {
    if (o instanceof KeyPair)
      return Optional.of(((KeyPair) o).getPrivate());
    if (o instanceof PrivateKey)
      return Optional.of((PrivateKey) o);
    return Optional.empty();
  }


  public static ErrorsOr<PrivateKey> readPemFile(Path pemFile) {
    try (PEMReader rdr = new PEMReader(new BufferedReader(new InputStreamReader(Files.newInputStream(pemFile), "UTF-8")))) {
      Optional<Object> o = Optional.ofNullable(rdr.readObject());
      if (!o.isPresent())
        return ErrorsOr.errors("The supplied file is not in PEM format.");
      Optional<PrivateKey> pk = extractPrivateKey(o.get());
      if (!pk.isPresent())
        return ErrorsOr.errors("The supplied file does not contain a private key.");
      return ErrorsOr.some(pk.get());
    } catch (IOException e) {
      log.error("Error while reading PEM file", e);
      return ErrorsOr.errors("Briefcase can't read the provided file: " + e.getMessage());
    }
  }

  public static void export(BriefcaseFormDefinition formDefinition, ExportConfiguration configuration, TerminationFuture terminationFuture) {
    if (formDefinition.isFileEncryptedForm() || formDefinition.isFieldEncryptedForm()) {
      formDefinition.setPrivateKey(readPemFile(configuration.getPemFile()
          .orElseThrow(() -> new RuntimeException("PEM file not present"))
      ).get());
    }

    Object action = null;
    if(configuration.getExportType().isPresent() && configuration.getExportType().get() == ExportType.STATA){
      // DTA - STATA
      action = new ExportToDta(
              terminationFuture,
              configuration.getExportDir().orElseThrow(() -> new RuntimeException("Export dir not present")).toFile(),
              formDefinition,
              formDefinition.getFormName(),
              true,
              false,
              configuration.mapStartDate((LocalDate ld) -> Date.from(ld.atStartOfDay(ZoneId.systemDefault()).toInstant())).orElse(null),
              configuration.mapEndDate((LocalDate ld) -> Date.from(ld.atStartOfDay(ZoneId.systemDefault()).toInstant())).orElse(null)
      );
    }
    else if(configuration.getExportType().isPresent() && configuration.getExportType().get() == ExportType.CSV){
      // CSV
      // 'overwrite' argument should be set to value of configuration.getOverwriteExistingFiles().get() instead of false by default.
      // however leaving as is until discussion & review with ODK folks
      action = new ExportToCsv(
              terminationFuture,
              configuration.getExportDir().orElseThrow(() -> new RuntimeException("Export dir not present")).toFile(),
              formDefinition,
              formDefinition.getFormName(),
              true,
              false,
              configuration.mapStartDate((LocalDate ld) -> Date.from(ld.atStartOfDay(ZoneId.systemDefault()).toInstant())).orElse(null),
              configuration.mapEndDate((LocalDate ld) -> Date.from(ld.atStartOfDay(ZoneId.systemDefault()).toInstant())).orElse(null)
      );
    }

    Method methodGetFormDefinition = null;
    Method method = null;

    try {
      if(action != null){
        method = action.getClass().getMethod("doAction", (Class<?>[]) null);
        boolean allSuccessful = (boolean) method.invoke(action, (Object[]) null);

        method = action.getClass().getMethod("getFormDefinition", (Class<?>[]) null);
        BriefcaseFormDefinition formDefinition1 = (BriefcaseFormDefinition) method.invoke(action, (Object[]) null);

        method = action.getClass().getMethod("noneSkipped", (Class<?>[]) null);
        boolean noneSkipped = (boolean) method.invoke(action, (Object[]) null);

        method = action.getClass().getMethod("someSkipped", (Class<?>[]) null);
        boolean someSkipped = (boolean) method.invoke(action, (Object[]) null);

        method = action.getClass().getMethod("allSkipped", (Class<?>[]) null);
        boolean allSkipped = (boolean) method.invoke(action, (Object[]) null);

        if (!allSuccessful){
          EventBus.publish(new ExportFailedEvent(formDefinition1));
        }

        if (allSuccessful && noneSkipped){
          EventBus.publish(new ExportSucceededEvent(formDefinition1));
        }

        if (allSuccessful && someSkipped){
          EventBus.publish(new ExportSucceededWithErrorsEvent(formDefinition1));
        }

        if (allSuccessful && allSkipped){
          EventBus.publish(new ExportFailedEvent(formDefinition1));
        }
      }

    } catch (IllegalAccessException | InvocationTargetException | SecurityException | NoSuchMethodException | ExportException n){
      log.error("export action failed", n);
      try {
        methodGetFormDefinition = action.getClass().getMethod("getFormDefinition", (Class<?>[]) null);
        BriefcaseFormDefinition formDefinition1 = (BriefcaseFormDefinition) methodGetFormDefinition.invoke(action, (Object[]) null);
        EventBus.publish(new ExportFailedEvent(formDefinition1));
      }catch (IllegalAccessException | InvocationTargetException | SecurityException | NoSuchMethodException | ExportException e) {
        log.error("export action failed", e);
      }
    }
  }

}
