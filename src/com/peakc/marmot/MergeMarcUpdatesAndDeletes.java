package com.peakc.marmot;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.log4j.Logger;
import org.ini4j.Ini;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.Record;


import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Merge a main marc export file with records from a delete and updates file
 * VuFind-Plus
 * User: Mark Noble
 * Date: 12/31/2014
 * Time: 11:45 AM
 *
 *
 */
public class MergeMarcUpdatesAndDeletes {
	private String recordNumberTag = "";
	private String recordNumberPrefix = "";

	public boolean startProcess(Ini configIni, Logger logger) {

		String mainFilePath = configIni.get("MergeUpdate", "marcPath");
		String backupPath = configIni.get("MergeUpdate", "backupPath");
		String marcEncoding = configIni.get("MergeUpdate", "marcEncoding");
		recordNumberTag = configIni.get("MergeUpdate", "recordNumberTag");
		recordNumberPrefix = configIni.get("MergeUpdate", "recordNumberPrefix");
		String additionsPath = configIni.get("MergeUpdate", "additionsPath");
		String deleteFilePath = configIni.get("MergeUpdate", "deleteFilePath");


		int numUpdates = 0;
		int numDeletions = 0;
		int numAdditions = 0;
		boolean errorOccurred = false;

		try {

			//Expect single main MARC file
			File mainFile = null;
			File[] files = new File(mainFilePath).listFiles();
			if(files != null) {
				for (File file : files) {
					if (file.getName().endsWith("mrc") || file.getName().endsWith("marc")) {
						mainFile = file;
						break;
					}
				}
			}

			if (mainFile != null) {

				//More than a one delete file
				HashSet<File> deleteFiles = new HashSet<>();
				files = new File(deleteFilePath).listFiles();
				if(files  != null && files.length >0 ){
					logger.info("File names to delete from:");
					for (File file : files) {
						if (file.getName().endsWith("mrc") || file.getName().endsWith("marc") || file.getName().endsWith("csv")) {
							deleteFiles.add(file);
							logger.info(file.getAbsolutePath());
						}
					}
				}

				//Expect files or directory
				HashSet<File> updateFiles = new HashSet<>();
				files = new File(additionsPath).listFiles();
				if(files  != null && files.length >0){
					logger.info("------------------------------");
					logger.info("File names to update from:");
					for (File file : files) {

						if(file.isDirectory()){
							File[] filesInDir = new File(file.getPath()).listFiles();
							if(filesInDir != null){
								for (File fileInDir: filesInDir){
									logger.info(fileInDir.getAbsolutePath());
									validateAddMarcFile(updateFiles, fileInDir);
								}
							}
						}
						else {
							logger.info(file.getName());
							validateAddMarcFile(updateFiles, file);
						}

					}
				}

				if ((deleteFiles.size() + updateFiles.size()) > 0) {
					HashMap<String, Record> recordsToUpdate = new HashMap<>();

					for (File updateFile : updateFiles) {

						try {
							processMarcFile(marcEncoding, recordsToUpdate, updateFile);
						} catch (Exception e) {

							logger.error("Error loading records from updates fail", e);
							errorOccurred = true;
						}
					}
					//Log the the records to update
					if (!recordsToUpdate.isEmpty()) {
						logger.info("Records to update.....");
						numUpdates = 0;
						for (Map.Entry<String, Record> entry : recordsToUpdate.entrySet()) {
							logger.info(entry.getKey());
							numUpdates++;
						}

						logger.info( "No of records to update: " + Integer.toString(numUpdates));
						logger.info("------------------------------");
					}
					else
						logger.info("No records to update.....");



					HashSet<String> recordsToDelete = new HashSet<>();

					for (File deleteFile : deleteFiles) {
						try {
							if (deleteFile.getName().endsWith("mrc") || deleteFile.getName().endsWith("marc")) {
								processMarcFile(marcEncoding, recordsToDelete, deleteFile);
							} else if (deleteFile.getName().endsWith("csv")) {
								processCsvFile(marcEncoding, recordsToDelete, deleteFile);
							}


						} catch (Exception e) {
							logger.error("Error processing deletes file", e);
							errorOccurred = true;

						}
					}
                  //Log the the records to delete
					if (!recordsToDelete.isEmpty()) {
						logger.info("Records to delete.....");
						numDeletions = 0;
						for (String entry : recordsToDelete) {
							logger.info(entry);
							numDeletions++;
						}

						logger.info("No of records to delete: "+ Integer.toString(numDeletions));
						logger.info("------------------------------");
					}
					else
						logger.info("No records to delete.....");

					String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
					File mergedFile = new File(mainFile.getPath() + "." + today + ".merged");
					Record curBib;

					logger.info("Merge started... pls wait");

					try {
						FileInputStream marcFileStream = new FileInputStream(mainFile);
						MarcReader mainReader = new MarcPermissiveStreamReader(marcFileStream, true, true, marcEncoding);

						FileOutputStream marcOutputStream = new FileOutputStream(mergedFile);
						MarcStreamWriter mainWriter = new MarcStreamWriter(marcOutputStream);
						while (mainReader.hasNext()) {
								curBib = mainReader.next();
								String recordId = getRecordIdFromMarcRecord(curBib);
								if (recordsToUpdate.containsKey(recordId)) {
									//Write the updated record
									mainWriter.write(recordsToUpdate.get(recordId));
									recordsToUpdate.remove(recordId);
									numUpdates++;
								} else if (!recordsToDelete.contains(recordId)) {
									//Unless the record is marked for deletion, write it
									mainWriter.write(curBib);
									numDeletions++;
								}
						}

						//Anything left in the updates file is new and should be added
						for (Record newMarc : recordsToUpdate.values()) {
							mainWriter.write(newMarc);
							numAdditions++;
						}
						mainWriter.close();
						marcFileStream.close();

						logger.info("Update Successfull!!");
					} catch (Exception e) {

						logger.error("Error processing main file", e);
						errorOccurred = true;
					}

					if (!new File(backupPath).exists()) {
						if (!new File(backupPath).mkdirs()) {
							logger.error("Could not create backup path");
							errorOccurred = true;

						}
					}
					if (!errorOccurred) {
						for (File updateFile : updateFiles) {
							try {
								//Move to the backup directory
								Util.copyFileNoOverwrite(updateFile, new File(backupPath));
							} catch (IOException e) {
								logger.error("Unable to move updates file " + updateFile.getAbsolutePath() + " to backup directory " + backupPath + "/" + updateFile.getName());
								errorOccurred = true;
							}

							if (!errorOccurred) {
								//safely delete the orginal file
								updateFile.delete();
							}
						}

						for (File deleteFile : deleteFiles) {
							//Move to the backup directory
							try {
								Util.copyFileNoOverwrite(deleteFile, new File(backupPath));
							} catch (IOException e) {
								logger.error("Unable to move delete file " + deleteFile.getAbsolutePath() + " to backup directory " + backupPath + "/" + deleteFile.getName());
								errorOccurred = true;
							}
							if (!errorOccurred) {
								//safely delete the orginal file
								deleteFile.delete();
							}
						}
					}


					if (!errorOccurred) {
						mainFilePath = mainFile.getPath();

						try {
							Util.copyFileNoOverwrite(mainFile, new File(backupPath));
						} catch (IOException e) {
							logger.error("Unable to move main file " + mainFile.getAbsolutePath() + " to backup directory " + backupPath + "/" + mainFile.getName());
							errorOccurred = true;
						}

						if (!errorOccurred) {
							mainFile.delete();

							//Move the merged file to the main file
							if (!mergedFile.renameTo(new File(mainFilePath))) {
								logger.error("Unable to move merged file to main file");
								errorOccurred = true;
							}
						}
					}
				} else {
					logger.info("No update or delete files were found");
					errorOccurred = true;
				}
			} else {
				logger.error("Did not find file to merge into");
				logger.error("No files were found in " + mainFilePath);
				errorOccurred = true;
			}
		} catch (Exception e) {
			logger.error("Unknown error merging records", e);
			errorOccurred = true;
		}
		return !errorOccurred;
	}

	private static void validateAddMarcFile(HashSet<File> updateFiles, File file) {
		if (file.getName().endsWith("mrc") || file.getName().endsWith("marc") || file.getName().endsWith("csv")) {
            updateFiles.add(file);
        }
	}

	private static void processCsvFile(String marcEncoding, HashSet<String> recordsToDelete, File deleteFile) throws IOException {

		CSVReader reader = new CSVReader(new FileReader(deleteFile.getPath()));

		String [] nextLine;
		while ((nextLine = reader.readNext()) != null) {
			recordsToDelete.add(nextLine[0]);
		}

		reader.close();
	}
	private void processMarcFile(String marcEncoding, HashSet<String> recordsToDelete, File deleteFile) throws IOException {
		FileInputStream marcFileStream = new FileInputStream(deleteFile);
		MarcReader deletesReader = new MarcPermissiveStreamReader(marcFileStream, true, true, marcEncoding);

		while (deletesReader.hasNext()) {
            Record curBib = deletesReader.next();
            String recordId = getRecordIdFromMarcRecord(curBib);
            if (recordId != null)
                recordsToDelete.add(recordId);
        }


		marcFileStream.close();
	}

	private void processMarcFile(String marcEncoding, HashMap<String, Record> recordsToUpdate, File updateFile) throws IOException {
		FileInputStream marcFileStream = new FileInputStream(updateFile);
		MarcReader updatesReader = new MarcPermissiveStreamReader(marcFileStream, true, true, marcEncoding);

		//Read a list of records in the updates file
		while (updatesReader.hasNext()) {
            Record curBib = updatesReader.next();
            String recordId = getRecordIdFromMarcRecord(curBib);

            if (recordsToUpdate != null)
                recordsToUpdate.put(recordId, curBib);
        }
		marcFileStream.close();
	}


	private String getRecordIdFromMarcRecord(Record marcRecord) {

		List<ControlField> recordIdField = getDataFields(marcRecord, recordNumberTag);
		if (recordIdField != null)
			return recordIdField.get(0).getData();

		return null;
	}


	private List<ControlField> getDataFields(Record marcRecord, String tag) {
		List variableFields = marcRecord.getVariableFields(tag);
		List<ControlField> variableFieldsReturn = new ArrayList<>();
		for (Object variableField : variableFields){
			if (variableField instanceof ControlField){
				variableFieldsReturn.add((ControlField)variableField);
			}
		}
		return variableFieldsReturn;
	}




}

