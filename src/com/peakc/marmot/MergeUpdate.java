package com.peakc.marmot;

import org.apache.log4j.Logger;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MergeUpdate {
    private static Logger logger = Logger.getLogger(MergeUpdate.class);
    // write your code here

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("The .ini configuration file must be provided as first parameter.");
            System.exit(1);
        }

        String configFileName = args[0];
        if (!configFileName.endsWith("ini")) {
            System.out.println("invalid .ini configuration");
            System.exit(1);
        }

        /*

        code for logging

        File log4jFile = new File("../../sites/" + serverName + "/conf/log4j.cron.properties");
        if (log4jFile.exists()){
            PropertyConfigurator.configure(log4jFile.getAbsolutePath());
        }else{
            System.out.println("Could not find log4j configuration " + log4jFile.toString());
        }

*/
        Ini configIni = loadConfigFile(args[0]);
        Date currentTime = new Date();
        logger.info(currentTime.toString() + ": Starting Merge");
        MergeMarcUpdatesAndDeletes merge = new MergeMarcUpdatesAndDeletes();
        if (merge.startProcess(configIni, logger)) {
            currentTime = new Date();
            logger.info(currentTime.toString() + ": Successful Merge");
        }
        else {
            currentTime = new Date();
            logger.info(currentTime.toString() + ": Merge Failed");
        }

    }
    private static Ini loadConfigFile(String filename){
        //First load the default config file
       /* String configName = "../../sites/default/conf/" + filename;*/
       /* logger.info("Loading configuration from " + configName);*/
        File configFile = new File(filename);
        if (!configFile.exists()) {
           /* logger.error("Could not find configuration file " + configName);*/
            System.exit(1);
        }

        // Parse the configuration file
        Ini ini = new Ini();
        try {
            ini.load(new FileReader(configFile));
        } catch (InvalidFileFormatException e) {
            /*logger.error("Configuration file is not valid.  Please check the syntax of the file.", e);*/
        } catch (FileNotFoundException e) {
            /*logger.error("Configuration file could not be found.  You must supply a configuration file in conf called config.ini.", e);*/
        } catch (IOException e) {
            /*logger.error("Configuration file could not be read.", e);*/
        }

       /*

        //Now override with the site specific configuration
        String siteSpecificFilename = "../../sites/" + serverName + "/conf/" + filename;
        *//*logger.info("Loading site specific config from " + siteSpecificFilename);*//*
        File siteSpecificFile = new File(siteSpecificFilename);
        if (!siteSpecificFile.exists()) {
          *//*  logger.error("Could not find server specific config file");*//*
            System.exit(1);
        }
        try {
            Ini siteSpecificIni = new Ini();
            siteSpecificIni.load(new FileReader(siteSpecificFile));
            for (Profile.Section curSection : siteSpecificIni.values()){
                for (String curKey : curSection.keySet()){
                    //logger.debug("Overriding " + curSection.getName() + " " + curKey + " " + curSection.get(curKey));
                    //System.out.println("Overriding " + curSection.getName() + " " + curKey + " " + curSection.get(curKey));
                    ini.put(curSection.getName(), curKey, curSection.get(curKey));
                }
            }
        } catch (InvalidFileFormatException e) {
           *//* logger.error("Site Specific config file is not valid.  Please check the syntax of the file.", e);*//*
        } catch (IOException e) {
            logger.error("Site Specific config file could not be read.", e);
        }

        //Now override with the site specific configuration
        String passwordFilename = "../../sites/" + serverName + "/conf/config.pwd.ini";
        *//*logger.info("Loading site specific config from " + siteSpecificFilename);*//*
        File siteSpecificPasswordFile = new File(passwordFilename);
        if (!siteSpecificPasswordFile.exists()) {
           *//* logger.error("Could not find server specific config password file");*//*
            System.exit(1);
        }
        try {
            Ini siteSpecificIni = new Ini();
            siteSpecificIni.load(new FileReader(siteSpecificPasswordFile));
            for (Profile.Section curSection : siteSpecificIni.values()){
                for (String curKey : curSection.keySet()){
                    //logger.debug("Overriding " + curSection.getName() + " " + curKey + " " + curSection.get(curKey));
                    //System.out.println("Overriding " + curSection.getName() + " " + curKey + " " + curSection.get(curKey));
                    ini.put(curSection.getName(), curKey, curSection.get(curKey));
                }
            }
        } catch (InvalidFileFormatException e) {
           *//* logger.error("Site Specific config file is not valid.  Please check the syntax of the file.", e);*//*
        } catch (IOException e) {
            *//*logger.error("Site Specific config file could not be read.", e);*//*
        }

        */

        return ini;
    }

    private static void StartMergeTest()
    {
        try {


            String recordNumberTag            = "907";
            String recordNumberPrefix         = ".b";

            File mainFile = new File("C:\\Marmot\\flatirons_ebrary\\bpl\\marc\\bpl_ebrary_marcrecords_20160420.mrc");

            String marcEncoding = "MARC8";


            String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
            File mergedFile = new File(mainFile.getPath() + "." + today + ".merged");
            int numRecordsRead = 0;
            String lastRecordId = "";
            Record curBib;
            try {
                FileInputStream marcFileStream = new FileInputStream(mainFile);
                MarcReader mainReader = new MarcPermissiveStreamReader(marcFileStream, true, true, marcEncoding);

                FileOutputStream marcOutputStream = new FileOutputStream(mergedFile);
                MarcStreamWriter mainWriter = new MarcStreamWriter(marcOutputStream);
                while (mainReader.hasNext()) {
                    curBib = mainReader.next();
                    String recordId = getRecordIdFromMarcRecord(curBib, recordNumberTag,recordNumberPrefix );
                    numRecordsRead++;

                    lastRecordId = recordId;
                }


                mainWriter.close();
                marcFileStream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }





    private static String getRecordIdFromMarcRecord(Record marcRecord, String recordNumberTag, String recordNumberPrefix ) {


        List<DataField> recordIdField = getDataFields(marcRecord,recordNumberTag);
        //Make sure we only get one ils identifier
        for (DataField curRecordField : recordIdField) {
            Subfield subfieldA = curRecordField.getSubfield('a');
            if (subfieldA != null && (recordNumberPrefix.length() == 0 || subfieldA.getData().length() > recordNumberPrefix.length())) {
                if (curRecordField.getSubfield('a').getData().substring(0, recordNumberPrefix.length()).equals(recordNumberPrefix)) {
                    return curRecordField.getSubfield('a').getData();
                }
            }
        }
        return null;
    }

    private static List<DataField> getDataFields(Record marcRecord, String tag) {
        List variableFields = marcRecord.getVariableFields(tag);
        List<DataField> variableFieldsReturn = new ArrayList<>();
        for (Object variableField : variableFields){
            if (variableField instanceof DataField){
                variableFieldsReturn.add((DataField)variableField);
            }
        }
        return variableFieldsReturn;
    }
}
