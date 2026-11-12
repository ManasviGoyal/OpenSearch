/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Utility class for checking file types
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FileTypeUtils {

    public static String BLOCK_FILE_IDENTIFIER = "_block_";
    public static String INDICES_FOLDER_IDENTIFIER = "index";
    /**
     * Suffix of the file an atomic-write download is written to before it is renamed to its final name. Distinct from
     * Lucene's {@code .tmp}, which the directories treat as a Lucene temp file.
     */
    public static final String PARTIAL_DOWNLOAD_SUFFIX = ".download";

    public static boolean isTempFile(String name) {
        return name.endsWith(".tmp");
    }

    /**
     * Checks if the file is a download that has not been renamed to its final name: incomplete, and never served.
     * @param name the file name
     * @return true if the name carries {@link #PARTIAL_DOWNLOAD_SUFFIX}
     */
    public static boolean isPartialDownloadFile(String name) {
        return name.endsWith(PARTIAL_DOWNLOAD_SUFFIX);
    }

    public static boolean isBlockFile(String name) {
        return name.contains(BLOCK_FILE_IDENTIFIER);
    }

    public static boolean isExtraFSFile(String name) {
        return name.startsWith("extra");
    }

    public static boolean isLockFile(String name) {
        return name.endsWith(".lock");
    }

    public static boolean isSegmentsFile(String name) {
        return name.startsWith("segments_");
    }

    /**
     * Checks if the file is a corrupted marker file.
     * @param name the file name
     * @return true if the file is a corrupted marker file
     */
    public static boolean isCorruptedFile(String name) {
        return name.startsWith("corrupted_");
    }
}
