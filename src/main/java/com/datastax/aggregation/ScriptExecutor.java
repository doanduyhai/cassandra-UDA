package com.datastax.aggregation;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

public class ScriptExecutor {

    private static final Logger SCRIPT_LOGGER = LoggerFactory.getLogger(ScriptExecutor.class);

    private static final String COMMA = ";";
    private static final String BATCH_BEGIN = "BEGIN";
    private static final String BATCH_APPLY = "APPLY";

    private static final String CODE_DELIMITER_START = "^\\s*(?:AS)?\\s*\\$\\$\\s*$";
    private static final String CODE_DELIMITER_END = "^\\s*\\$\\$\\s*;\\s*$";

    private final Session session;

    public ScriptExecutor(Session session) {
        this.session = session;
    }

    /**
     * Execute a CQL script file located in the class path
     * @param scriptLocation
     *      the location of the script file in the class path
     */
    public void executeScript(String scriptLocation) {
        final List<SimpleStatement> statements = buildStatements(loadScriptAsLines(scriptLocation));
        for (SimpleStatement statement : statements) {
            SCRIPT_LOGGER.debug("\tSCRIPT : {}", statement.getQueryString());
            session.execute(statement);
        }
    }

    /**
     * Execute a plain CQL string statement
     * @param statement
     *      plain CQL string statement
     *
     * @return the resultSet
     *
     */
    public ResultSet execute(String statement) {
        return session.execute(statement);
    }

    /**
     * Execute a CQL statement
     * @param statement
     *      CQL statement
     *
     * @return the resultSet
     *
     */
    public ResultSet execute(Statement statement) {
        return session.execute(statement);
    }

    /**
     * Execute a plain CQL string statement asynchronously
     *
     * @param statement the CQL string statement
     *
     * @return Future&lt;ResultSet&gt;
     */
    public Future<ResultSet> executeAsync(String statement) {
        return session.executeAsync(statement);
    }

    /**
     * Execute a CQL statement asynchronously
     *
     * @param statement CQL statement
     *
     * @return Future&lt;ResultSet&gt;
     */
    public Future<ResultSet> executeAsync(Statement statement) {
        return session.executeAsync(statement);
    }

    protected List<String> loadScriptAsLines(String scriptLocation) {

        InputStream inputStream = this.getClass().getResourceAsStream("/" + scriptLocation);


        Scanner scanner = new Scanner(inputStream);
        List<String> lines = new ArrayList<>();
        while (scanner.hasNextLine()) {
            final String nextLine = scanner.nextLine().trim();
            if (isNotBlank(nextLine)) {
                lines.add(nextLine);
            }
        }
        return lines;
    }

    protected List<SimpleStatement> buildStatements(List<String> lines) {
        List<SimpleStatement> statements = new ArrayList<>();
        StringBuilder statement = new StringBuilder();
        boolean batch = false;
        boolean codeBlock = false;
        StringBuilder batchStatement = new StringBuilder();
        for (String line : lines) {
            if (line.trim().startsWith(BATCH_BEGIN)) {
                batch = true;
            }
            if (line.trim().matches(CODE_DELIMITER_START)) {
                if(codeBlock) {
                    codeBlock = false;
                } else {
                    codeBlock = true;
                }
            }

            if (batch) {
                batchStatement.append(line);
                if (line.trim().startsWith(BATCH_APPLY)) {
                    batch = false;
                    statements.add(new SimpleStatement(statement.toString()));
                    batchStatement = new StringBuilder();
                }
            } else if(codeBlock) {
                statement.append(line);
                if (line.trim().matches(CODE_DELIMITER_END)) {
                    codeBlock = false;
                    statements.add(new SimpleStatement(statement.toString()));
                    statement = new StringBuilder();
                }
            }
            else {
                statement.append(line);
                if (line.trim().endsWith(COMMA)) {
                    statements.add(new SimpleStatement(statement.toString()));
                    statement = new StringBuilder();
                } else {
                    statement.append(" ");
                }
            }

        }
        return statements;
    }
}