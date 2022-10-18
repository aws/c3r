// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.c3r.io.sql;

import com.amazonaws.c3r.exception.C3rRuntimeException;
import lombok.Getter;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * A SQL connection and the underlying database file.
 */
@Getter
public class SqlTable {
    /**
     * Connection to the SQL database for this session.
     */
    private final Connection connection;

    /**
     * SQL database file.
     */
    private final File databaseFile;

    /**
     * Creates a connection to a SQL database using the file as the database source.
     *
     * @param databaseFile File to use as a SQL database
     * @throws C3rRuntimeException If there's an error accessing file
     */
    public SqlTable(final File databaseFile) {
        try {
            this.connection = DriverManager.getConnection("jdbc:sqlite:" + databaseFile.getAbsolutePath());
        } catch (SQLException e) {
            throw new C3rRuntimeException("Could not access SQL database.", e);
        }
        this.databaseFile = databaseFile;
    }
}
