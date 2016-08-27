package com.peakc.marmot;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.log4j.Logger;
import org.ini4j.Ini;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.Record;

import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/*import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;*/

/*
import org.apache.log4j.Logger;
import org.ini4j.Ini;
import org.ini4j.Profile;
import org.marc4j.*;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.*;

* */





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
					logger.info("File name(s) to delete from:");
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
					logger.info("File name(s) to update from:");
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
							logger.info(file.getAbsolutePath());
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


					logger.info("Merge started... pls wait");
					numDeletions = 0;
					numAdditions = 0;
					numUpdates = 0;
					try {
						FileInputStream marcFileStream = new FileInputStream(mainFile);
						MarcReader mainReader = new MarcPermissiveStreamReader(marcFileStream, true, true, marcEncoding);
						Record curBib;
						FileOutputStream marcOutputStream = new FileOutputStream(mergedFile);
						MarcStreamWriter mainWriter = new MarcStreamWriter(marcOutputStream);
						while (mainReader.hasNext()) {
							try{
								curBib = mainReader.next();
								String recordId = getRecordIdFromMarcRecord(curBib);
								if(recordId == null)
									continue;

								if (recordsToUpdate.containsKey(recordId)) {
									//Write the updated record
									mainWriter.write(recordsToUpdate.get(recordId));
									recordsToUpdate.remove(recordId);
									numUpdates++;
									logger.info("Updating... " + recordId);
								}else if(recordsToDelete.contains(recordId)){
									 numDeletions++;
									logger.info("Deleting..." + recordId);
								} else if (!recordsToDelete.contains(recordId)) {
									//Unless the record is marked for deletion, write it
									mainWriter.write(curBib);
								}
							}catch(NoClassDefFoundError ex){
								logger.error(ex.getMessage());

							}
						}

						//Anything left in the updates file is new and should be added
						for (Record newMarc : recordsToUpdate.values()) {
							mainWriter.write(newMarc);
							logger.info("Adding...." + getRecordIdFromMarcRecord(newMarc));
							numAdditions++;
						}
						mainWriter.close();
						marcFileStream.close();

						logger.info("Additions: " + numAdditions);
						logger.info("Deletions: " + numDeletions);
						logger.info("Updates: " + numUpdates);

						logger.info("Update SUCCESSFUL");
					} catch (Exception e) {

						logger.error("Error processing main file", e);
						errorOccurred = true;
					}

					//if no processing error occurred go ahead and backup original files
					if (!errorOccurred) {

						for (File updateFile : updateFiles) {
							//Move to the backup directory
							if(!BackUpFile(updateFile, backupPath)){
								logger.error("Unable to move update file " + updateFile.getAbsolutePath() + " to backup directory " + backupPath + "/" + updateFile.getName());
							}
						}

						for (File deleteFile : deleteFiles) {
						//Move to the backup directory
							if(!BackUpFile(deleteFile, backupPath)){
								logger.error("Unable to move update file " + deleteFile.getAbsolutePath() + " to backup directory " + backupPath + "/" + deleteFile.getName());
							}
						}

						//Move the original maim file into back up folder
						if(BackUpFile(mainFile, backupPath)){
							//rename the merged file to the main file
							if (!mergedFile.renameTo(new File(mainFile.getPath()))) {
								logger.error("Unable to rename merged (updated) file to main file. Manual renaming may be necessary!");
							}
						}
						else{
							logger.error("Unable to move main file " + mainFile.getAbsolutePath() + " to backup directory " + backupPath + "/" + mainFile.getName());
						}
					}

				} else {
					logger.info("No update or delete files were found");
					errorOccurred = true;
				}
			} else {
				logger.info("Did not find file to merge into");
				logger.info("No master file was found in " + mainFilePath);
				errorOccurred = true;
			}
		} catch (Exception e) {
			logger.error("Unknown error merging records", e);
			errorOccurred = true;
		}

		if(errorOccurred) {
			//cleanup temp files
			//if failure occurs
			String today = new SimpleDateFormat("yyyyMMdd").format(new Date());
			File tmpMergedFile = new File(mainFilePath + "." + today + ".merged");
			if(tmpMergedFile.exists())
				tmpMergedFile.delete();
		}

		return !errorOccurred;
	}

	private static boolean BackUpFile(File fileToMove, String backupPath) {
		boolean fileBackedUp;
		try {
			fileBackedUp = ArchiveFile(fileToMove, backupPath);
		} catch (IOException e) {
			return false;
		}

		return fileBackedUp;
	}
	//returns true if file is safely moved to backup location
	private static boolean ArchiveFile(File fileToMove, String backUpPath)throws IOException {

		boolean fileMoved = false;

		if (new File(backUpPath).exists() || new File(backUpPath).mkdirs()) {

			try {
				fileMoved = fileToMove.renameTo(new File(backUpPath));
			} catch (Exception e) {
				fileMoved = false;
			} finally {
				if (!fileMoved) {
					//try copying over then deleting the original
					try {
						CopyNoOverwriteResult fileCopiedRes = Util.copyFileNoOverwrite(fileToMove, new File(backUpPath));
						fileToMove.delete();
						fileMoved = true;
					} catch (IOException e) {
						//unable to copy over the file
						throw new IOException("Unable to backup file :" + fileToMove.getAbsolutePath() + "to " + backUpPath);
					}
				}
			}
		} else {
			throw new IOException("BackUp folder is non existent or cannot be created!");
		}


		return fileMoved;
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

			if (recordsToUpdate != null && recordId != null)
				recordsToUpdate.put(recordId, curBib);
		}
		marcFileStream.close();
	}


	private String getRecordIdFromMarcRecord(Record marcRecord) {

		List<ControlField> recordIdField = getDataFields(marcRecord, recordNumberTag);

		if (recordIdField != null && recordIdField.size() > 0  )
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

