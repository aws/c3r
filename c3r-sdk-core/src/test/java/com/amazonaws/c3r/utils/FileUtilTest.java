// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.amazonaws.c3r.utils.FileUtil.isWindows;
import static com.amazonaws.c3r.utils.FileUtil.setWindowsFilePermissions;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileUtilTest {

    @Test
    public void verifyBlankLocationRejectedByVerifyReadableFile() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyReadableFile(""));
    }

    @Test
    public void verifyDirectoryRejectedByVerifyReadableFile() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyReadableFile(FileUtil.TEMP_DIR));
    }

    @Test
    public void verifyMissingFileRejectedByVerifyReadableFile() throws IOException {
        final String file = FileTestUtility.resolve("missing.csv").toString();
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyReadableFile(file));
    }

    @Test
    public void verifyNoReadPermissionsRejectedByVerifyReadableFile() throws IOException {
        final File file = FileTestUtility.createTempFile("NotReadable", ".tmp").toFile();
        if (isWindows()) {
            setWindowsFilePermissions(file.toPath(), AclEntryType.DENY, Set.of(AclEntryPermission.READ_DATA));
        } else {
            assertTrue(file.setReadable(false, false));
        }
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyReadableFile(file.getAbsolutePath()));
    }

    @Test
    public void verifyFileAcceptedByVerifyReadableFile() throws IOException {
        final File file = FileTestUtility.createTempFile("Readable", ".tmp").toFile();
        if (isWindows()) {
            setWindowsFilePermissions(file.toPath(), AclEntryType.ALLOW, Set.of(AclEntryPermission.READ_DATA));
        } else {
            assertTrue(file.setReadable(true, true));
        }
        assertDoesNotThrow(() -> FileUtil.verifyReadableFile(file.getAbsolutePath()));
    }

    @Test
    public void verifyBlankLocationRejectedByVerifyReadableDirectory() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyReadableDirectory(""));
    }

    @Test
    public void verifyFileRejectedByVerifyReadableDirectory() {
        assertThrowsExactly(C3rIllegalArgumentException.class,
                () -> FileUtil.verifyReadableDirectory(FileTestUtility.createTempFile().toString()));
    }

    @Test
    public void verifyMissingDirectoryRejectedByVerifyReadableDirectory() throws IOException {
        final String file = FileTestUtility.resolve("directory").toString();
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyReadableDirectory(file));
    }

    @Test
    public void verifyNoReadPermissionsRejectedByVerifyReadableDirectory() throws IOException {
        final File file = FileTestUtility.createTempDir().toFile();
        if (isWindows()) {
            setWindowsFilePermissions(file.toPath(), AclEntryType.DENY, Set.of(AclEntryPermission.READ_DATA));
        } else {
            assertTrue(file.setReadable(false, false));
        }
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyReadableDirectory(file.getAbsolutePath()));
    }

    @Test
    public void verifyDirectoryAcceptedByVerifyReadableDirectory() throws IOException {
        final File file = FileTestUtility.createTempDir().toFile();
        if (isWindows()) {
            setWindowsFilePermissions(file.toPath(), AclEntryType.ALLOW, Set.of(AclEntryPermission.READ_DATA));
        } else {
            assertTrue(file.setReadable(true, true));
        }
        assertDoesNotThrow(() -> FileUtil.verifyReadableDirectory(file.getAbsolutePath()));
    }

    @Test
    public void verifyBlankLocationRejectedByVerifyWritableFile() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyWritableFile("", false));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyWritableFile("", true));
    }

    @Test
    public void verifyDirectoryRejectedByVerifyFileWriteable() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyWritableFile(FileUtil.TEMP_DIR, false));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyWritableFile(FileUtil.TEMP_DIR, true));
    }

    @Test
    public void verifyFileWithoutWritePermissionsRejectedByVerifyFileWriteable() throws IOException {
        final File file = FileTestUtility.createTempFile("NotWriteable", ".tmp").toFile();
        if (isWindows()) {
            setWindowsFilePermissions(file.toPath(), AclEntryType.DENY, Set.of(AclEntryPermission.WRITE_DATA));
        } else {
            assertTrue(file.setWritable(false, false));
        }
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyWritableFile(file.getAbsolutePath(), false));
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyWritableFile(file.getAbsolutePath(), true));
    }

    @Test
    public void verifyNewFileAcceptedByVerifyFileWriteable() throws IOException {
        final String file = FileTestUtility.resolve("newFile.csv").toString();
        assertDoesNotThrow(() -> FileUtil.verifyWritableFile(file, false));
        assertDoesNotThrow(() -> FileUtil.verifyWritableFile(file, true));
    }

    @Test
    public void verifyExistingFileAndNoOverwriteRejectedByVerifyFileWriteable() throws IOException {
        final String file = FileTestUtility.createTempFile().toString();
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyWritableFile(file, false));
    }

    @Test
    public void verifyExistingFileAndOverwriteAcceptedByVerifyFileWriteable() throws IOException {
        final String file = FileTestUtility.createTempFile().toString();
        assertDoesNotThrow(() -> FileUtil.verifyWritableFile(file, true));
    }

    @Test
    public void verifyBlankLocationRejectedByVerifyWriteableDirectory() {
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyWritableDirectory(""));
    }

    @Test
    public void verifyFileIsRejectedByVerifyWriteableDirectory() throws IOException {
        final String file = FileTestUtility.createTempFile().toString();
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyWritableDirectory(file));
    }

    @Test
    public void verifyDirectoryWithoutWritePermissionsRejectedByVerifyWriteableDirectory() throws IOException {
        final File dir = FileTestUtility.createTempDir().toFile();
        if (isWindows()) {
            setWindowsFilePermissions(dir.toPath(), AclEntryType.DENY, Set.of(AclEntryPermission.WRITE_DATA));
        } else {
            assertTrue(dir.setWritable(false, false));
        }
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyWritableDirectory(dir.getAbsolutePath()));
    }

    @Test
    public void verifyDirectoryAcceptedByVerifyWriteableDirectory() throws IOException {
        final String dir = FileTestUtility.createTempDir().toFile().toString();
        assertDoesNotThrow(() -> FileUtil.verifyWritableDirectory(dir));
    }

    @Test
    public void verifyNewDirectoryAcceptedByVerifyWriteableDirectory() throws IOException {
        final String dir = FileTestUtility.createTempDir().toString();
        assertDoesNotThrow(() -> FileUtil.verifyWritableDirectory(dir));
    }

    @Test
    public void verifyExistingDirectoryAndNoOverwriteRejectedByVerifyDirectoryWriteable() throws IOException {
        final String file = FileTestUtility.createTempDir().toString();
        assertThrowsExactly(C3rIllegalArgumentException.class, () -> FileUtil.verifyWritableDirectory(file, false));
    }

    @Test
    public void verifyExistingDirectoryAndOverwriteAcceptedByVerifyDirectoryWriteable() throws IOException {
        final String file = FileTestUtility.createTempDir().toString();
        assertDoesNotThrow(() -> FileUtil.verifyWritableDirectory(file, true));
    }

    @Test
    public void initFileIfNotExistsTest() throws IOException {
        final File tempFile = FileTestUtility.resolve("output.csv").toFile();

        assertFalse(tempFile.exists());

        FileUtil.initFileIfNotExists(tempFile.getAbsolutePath());

        assertTrue(tempFile.exists());
        if (!isWindows()) {
            assertTrue(tempFile.canWrite());
            assertTrue(tempFile.canRead());
        } else {
            verifyWindowsPermissions(tempFile.toPath(), Set.of(AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA));
        }
    }

    @Test
    public void initFileIfNotExistsRespectsExistingPermissionsTest() throws IOException {
        final File tempFile = FileTestUtility.createTempFile().toFile();
        if (isWindows()) {
            setWindowsFilePermissions(tempFile.toPath(), AclEntryType.ALLOW, Set.of(AclEntryPermission.READ_DATA,
                    AclEntryPermission.EXECUTE));
        } else {
            assertTrue(tempFile.setReadable(true, true));
            assertTrue(tempFile.setExecutable(true, true));
            assertTrue(tempFile.setWritable(false, false));
        }

        FileUtil.initFileIfNotExists(tempFile.getAbsolutePath());

        assertTrue(tempFile.exists());
        if (!isWindows()) {
            assertFalse(tempFile.canWrite());
            assertTrue(tempFile.canRead());
            assertTrue(tempFile.canExecute());
        } else {
            verifyWindowsPermissions(tempFile.toPath(), Set.of(AclEntryPermission.READ_DATA, AclEntryPermission.EXECUTE));
        }
    }

    @Test
    public void readWriteOnlyFilePermissionsTest() throws IOException {
        final File tempFile = FileTestUtility.createTempFile().toFile();
        FileUtil.setOwnerReadWriteOnlyPermissions(tempFile);
        assertTrue(tempFile.exists());

        if (!isWindows()) {
            assertTrue(tempFile.canWrite());
            assertTrue(tempFile.canRead());
        } else {
            verifyWindowsPermissions(tempFile.toPath(), Set.of(AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA));
        }
    }

    @Test
    public void initFileIfNotExistsFilePathTooLongOnWindowsTest() {
        if (isWindows()) {
            final byte[] filePathBytes = new byte[500];
            Arrays.fill(filePathBytes, (byte) 'a');
            final String longFilePath = new String(filePathBytes, StandardCharsets.UTF_8);
            assertThrows(C3rRuntimeException.class, () -> FileUtil.initFileIfNotExists(longFilePath));
        }
    }

    @SuppressWarnings("unchecked")
    private void verifyWindowsPermissions(final Path path, final Set<AclEntryPermission> permissions) throws IOException {
        final List<AclEntry> acls = (List<AclEntry>) Files.getAttribute(path, "acl:acl");

        final AclEntryPermission[] actual = acls.get(0).permissions().toArray(new AclEntryPermission[0]);
        Arrays.sort(actual);
        final AclEntryPermission[] expected = permissions == null ? new AclEntryPermission[0] :
                permissions.toArray(new AclEntryPermission[0]);
        Arrays.sort(expected);
        assertArrayEquals(expected, actual);
    }
}