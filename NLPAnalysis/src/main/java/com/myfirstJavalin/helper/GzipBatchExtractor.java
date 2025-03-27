package com.myfirstJavalin.helper;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class GzipBatchExtractor {

    public static void unzipFile(File sourceFile, File targetDir) throws IOException {
        String outputFileName = sourceFile.getName().replace(".xmi.gz", ".xmi"); // Change extension
        File targetFile = new File(targetDir, outputFileName);

        try (GZIPInputStream gis = new GZIPInputStream(new FileInputStream(sourceFile));
             FileOutputStream fos = new FileOutputStream(targetFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos)) {

            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = gis.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
        }
        System.out.println("Extracted: " + sourceFile.getName() + " -> " + targetFile.getAbsolutePath());
    }

    public static void unzipAllFilesInDirectory(String sourceDirPath, String targetDirPath) {
        File sourceDir = new File(sourceDirPath);
        File targetDir = new File(targetDirPath);

        if (!sourceDir.exists() || !sourceDir.isDirectory()) {
            System.err.println("Source directory does not exist or is not a directory.");
            return;
        }
        if (!targetDir.exists()) {
            targetDir.mkdirs(); // Create target directory if it doesn't exist
        }

        File[] gtzFiles = sourceDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".xmi.gz"));
        if (gtzFiles == null || gtzFiles.length == 0) {
            System.err.println("No .gtz files found in the directory.");
            return;
        }

        for (File gtzFile : gtzFiles) {
            try {
                unzipFile(gtzFile, targetDir);
            } catch (IOException e) {
                System.err.println("Error extracting: " + gtzFile.getName());
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        String sourceDirPath = "/Users/solo/Downloads/uebung4-main/testUebung5/src/main/resources/20";  // Change this to your source directory
        String targetDirPath = "/Users/solo/Downloads/uebung4-main/testUebung5/src/main/resources/processedXml"; // Change this to your output directory

        unzipAllFilesInDirectory(sourceDirPath, targetDirPath);
    }
}