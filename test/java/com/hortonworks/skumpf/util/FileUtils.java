package com.hortonworks.skumpf.util;

import java.io.File;

/**
 * Created by skumpf on 12/31/14.
 */
public class FileUtils {

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    System.out.println("KAFKA: Deleting " + f.getAbsolutePath());
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
}
