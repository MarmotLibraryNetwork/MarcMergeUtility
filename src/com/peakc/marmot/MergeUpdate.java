package com.peakc.marmot;

import org.apache.log4j.Logger;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;


import java.io.*;;
import java.util.Date;


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

        File configFile = new File(filename);
        if (!configFile.exists()) {
            logger.error("Could not find configuration file " + filename);
            System.exit(1);
        }

        // Parse the configuration file
        Ini ini = new Ini();
        try {
            ini.load(new FileReader(configFile));
        } catch (InvalidFileFormatException e) {
            logger.error("Configuration file is not valid.  Please check the syntax of the file.", e);
            System.exit(1);
        } catch (FileNotFoundException e) {
            logger.error("Configuration file could not be found.  You must supply a configuration file in conf called config.ini.", e);
            System.exit(1);
        } catch (IOException e) {
            logger.error("Configuration file could not be read.", e);
            System.exit(1);
        }


        return ini;
    }

}