// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.utils;

import com.amazonaws.c3r.exception.C3rIllegalArgumentException;
import com.amazonaws.c3r.exception.C3rRuntimeException;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Utility class for file related I/O actions.
 */
public final class FileUtil {
    /**
     * Directory executable is running from.
     */
    public static final String CURRENT_DIR = System.getProperty("user.dir");

    /**
     * System default temporary directory location.
     */
    public static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    /**
     * Hidden utility class constructor.
     */
    private FileUtil() {
    }

    /**
     * Reads bytes from a source file into a String.
     *
     * @param file source filename
     * @return source file content
     * @throws C3rIllegalArgumentException If the filepath is invalid
     * @throws C3rRuntimeException         If there's an error while reading file
     */
    public static String readBytes(final String file) {
        final Path p;
        try {
            p = FileSystems.getDefault().getPath(file);
        } catch (InvalidPathException e) {
            throw new C3rIllegalArgumentException("Failed to open file: " + file + ".");
        }
        final String bytes;
        try {
            // Note: Do not replace with `Files.readString()` as your IDE may suggest. Our encrypted values frequently involve
            // padding (`=` characters at the end of the string) to make the binary values be even blocks of four. `Files.readString()`
            // tries to convert the entire string, including the padding into UTF-8 which generates a malformed input exception.
            // The constructor for the String class takes into account padding and removes it (specifically, it always replaces
            // malformed input and unmappable character sequences with this charset's default replacement string). This means it always
            // decodes the value, regardless of padding. If you do change to `Files.readString()`, unit tests will fail due to the errors.
            bytes = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new C3rRuntimeException("Failed to read file: " + file + ".");
        }
        return bytes;
    }

    /**
     * Checks if a location is readable and a file.
     *
     * @param location Filepath
     * @throws C3rIllegalArgumentException If the filepath is empty, points to an invalid location or is not readable
     */
    public static void verifyReadableFile(final String location) {
        if (location.isBlank()) {
            throw new C3rIllegalArgumentException("File path is empty.");
        }

        // Check that it's a file, not a directory, in a readable location
        final Path inFile = Path.of(location);
        if (Files.isDirectory(inFile)) {
            throw new C3rIllegalArgumentException("Cannot read from file `" + location + "`. File is a directory.");
        } else if (Files.notExists(inFile)) {
            throw new C3rIllegalArgumentException("Cannot read from file `" + location + "`. File does not exist.");
        } else if (!Files.isReadable(inFile)) {
            throw new C3rIllegalArgumentException("Cannot read from file `" + location + "`. Permission denied.");
        }
    }

    /**
     * Checks if a location is readable and a directory.
     *
     * @param location Filepath
     * @throws C3rIllegalArgumentException If the filepath is empty, points to an invalid location or is not readable
     */
    public static void verifyReadableDirectory(final String location) {
        if (location.isBlank()) {
            throw new C3rIllegalArgumentException("File path is empty.");
        }

        // Check that it's a directory in a readable location
        final Path inFile = Path.of(location);
        if (!Files.isDirectory(inFile)) {
            throw new C3rIllegalArgumentException("Cannot read from directory `" + location + "`. File is not a directory.");
        } else if (Files.notExists(inFile)) {
            throw new C3rIllegalArgumentException("Cannot read from directory `" + location + "`. File does not exist.");
        } else if (!Files.isReadable(inFile)) {
            throw new C3rIllegalArgumentException("Cannot read from directory `" + location + "`. Permission denied.");
        }
    }

    /**
     * Checks if a location is writable. If the file already exists, the location is only considered writable if the
     * {@code override} flag is {@code true}. Directories are not considered files.
     *
     * @param location  Filepath to check
     * @param overwrite Indicates if we can overwrite an existing file
     * @throws C3rIllegalArgumentException If the filepath is empty, points to an invalid location or is not a writable location
     */
    public static void verifyWritableFile(final String location, final boolean overwrite) {
        if (location.isBlank()) {
            throw new C3rIllegalArgumentException("File path is empty.");
        }

        // Check if file exists and can be written to
        final Path outFile = Path.of(location);
        if (Files.exists(outFile) && !overwrite) {
            throw new C3rIllegalArgumentException(
                    "Cannot write to file `" + location + "`. File already exists and overwrite flag is false.");
        } else if (Files.isDirectory(outFile)) {
            throw new C3rIllegalArgumentException("Cannot write to file `" + location + "`. File is a directory.");
        } else if (Files.exists(outFile) && !Files.isWritable(outFile)) {
            throw new C3rIllegalArgumentException("Cannot write to file `" + location + "`. Permission denied.");
        }
    }

    /**
     * Checks if a location is a writable directory. Directory must already exist.
     *
     * @param location Filepath to check
     * @throws C3rIllegalArgumentException If the filepath is empty, not a directory or is not a writable location
     */
    public static void verifyWritableDirectory(final String location) {
        verifyWritableDirectory(location, true);
    }

    /**
     * Checks if a location is a writable directory. Directory must already exist.
     *
     * @param location Filepath to check
     * @param overwrite Indicates if we can overwrite an existing path
     * @throws C3rIllegalArgumentException If the filepath is empty, not a directory or is not a writable location
     */
    public static void verifyWritableDirectory(final String location, final boolean overwrite) {
        if (location.isBlank()) {
            throw new C3rIllegalArgumentException("File path is empty.");
        }
        // Check that it's a writeable directory
        final Path outFileDirectory = Path.of(location);
        if (Files.exists(outFileDirectory) && !overwrite) {
            throw new C3rIllegalArgumentException(
                    "Cannot write to path `" + location + "`. path already exists and overwrite flag is false.");
        } else if (Files.exists(outFileDirectory) && !Files.isDirectory(outFileDirectory)) {
            throw new C3rIllegalArgumentException("Cannot write to path `" + location + "`. Path is not a directory.");
        } else if (Files.exists(outFileDirectory) && !Files.isWritable(outFileDirectory)) {
            throw new C3rIllegalArgumentException("Cannot write to path `" + location + "`. Permission denied.");
        }
    }

    /**
     * Creates the file passed in if it does not exist and sets RW permissions only for the owner.
     *
     * @param fileName the file to be created
     * @throws C3rRuntimeException If the file could not be initialized
     */
    public static void initFileIfNotExists(final String fileName) {
        final Path file = Path.of(fileName);
        if (!Files.exists(file)) {
            // create the target file and set RO permissions
            try {
                Files.createFile(file);
                setOwnerReadWriteOnlyPermissions(file.toFile());
            } catch (IOException e) {
                throw new C3rRuntimeException("Cannot initialize file: " + fileName, e);
            }
        }
    }

    /**
     * Creates the directory passed in if it does not exist and sets RW permissions only for the owner.
     *
     * @param directoryName the directory to be created
     * @throws C3rRuntimeException If the file could not be initialized
     */
    public static void initDirectoryIfNotExists(final String directoryName) {
        final Path file = Path.of(directoryName);
        if (!Files.exists(file)) {
            // create the target file and set RO permissions
            try {
                Files.createDirectory(file);
            } catch (IOException e) {
                throw new C3rRuntimeException("Cannot initialize directory: " + directoryName, e);
            }
        }
    }

    /**
     * Sets permissions to RW for the owner only on the passed file.
     *
     * @param file Change file permissions at this location
     * @throws C3rRuntimeException If the file permissions couldn't be set
     */
    public static void setOwnerReadWriteOnlyPermissions(@NonNull final File file) {
        if (isWindows()) {
            setWindowsFilePermissions(file.toPath(), AclEntryType.ALLOW, Set.of(AclEntryPermission.READ_DATA,
                    AclEntryPermission.WRITE_DATA));
        } else {
            boolean success = file.setWritable(false, false);
            success &= file.setWritable(true, true);
            success &= file.setReadable(false, false);
            success &= file.setReadable(true, true);
            success &= file.setExecutable(false, false);
            if (!success) {
                throw new C3rRuntimeException("Unable to set permissions on file: " + file.getPath());
            }
        }
    }

    /**
     * Set the file to the specified permissions on a computer running the Windows operating system.
     *
     * @param path        Change file permissions at this location
     * @param type        Access control list entry type
     * @param permissions Requested permissions
     * @throws C3rRuntimeException If the file permissions couldn't be set
     */
    static void setWindowsFilePermissions(final Path path, final AclEntryType type, final Set<AclEntryPermission> permissions) {
        try {
            final UserPrincipal owner = Files.getOwner(path);
            final AclEntry entry = AclEntry.newBuilder()
                    .setType(type)
                    .setPrincipal(owner)
                    .setPermissions(permissions)
                    .build();
            final AclFileAttributeView view = Files.getFileAttributeView(path, AclFileAttributeView.class);
            final List<AclEntry> acl = view.getAcl() == null ? new ArrayList<>() : view.getAcl();
            acl.add(0, entry);

            Files.setAttribute(path, "acl:acl", acl);
        } catch (IOException e) {
            throw new C3rRuntimeException("Could not set file permissions for file: " + path, e);
        }
    }

    /**
     * Checks if the system is running the Windows operating system.
     *
     * @return {@code true} if the OS is Windows
     */
    public static boolean isWindows() {
        return System.getProperty("os.name").startsWith("Windows");
    }
}
