package com.adwords.google.desj;


import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class AWQLParser {
    private String awql = null;
    private String fields = null;
    private List<String> fieldList = null;
    private String reportType = null;

    public AWQLParser() {
    }

    public static String buildRequestFormat(String awql) {
        awql = awql.replace('\n', ' ');
        awql = awql.replace('\r', ' ');
        awql = awql.replace('\t', ' ');
        awql = reduceMultipleSpacesToOne(awql);
        awql = awql.replace(", ", ",");
        awql = awql.replace(" ,", ",");
        return awql;
    }

    private static String reduceMultipleSpacesToOne(String text) {
        text = text.trim();

        for (int pos = 0; pos != -1; pos = text.indexOf("  ")) {
            text = text.replace("  ", " ");
        }

        return text;
    }

    public AWQLParser parse(String awql) {
        awql = awql.trim();
        findFields();
        findReportType();
        return this;
    }

    private void findReportType() {
        int pos0 = awql.toLowerCase().indexOf("from");
        if (pos0 == -1) {
            throw new IllegalStateException("The given AWQL does not contains a \"from\" clause. The \"from\" clause is mandatory in AdHock reports!");
        } else {
            pos0 += "from".length();
            int pos1 = awql.toLowerCase().indexOf("where");
            if (pos1 == -1) {
                pos1 = awql.toLowerCase().indexOf("during");
            }

            if (pos1 == -1) {
                throw new IllegalStateException("The given AWQL does not contains a \"during\" clause. The \"during\" clause is mandatory in AdHock reports!");
            } else {
                reportType = awql.substring(pos0, pos1).trim();
                if (reportType.isEmpty()) {
                    throw new IllegalStateException("The given AWQL does not contains a report-type in the \"from\" clause!");
                }
            }
        }
    }

    private void findFields() {
        int pos0 = awql.toLowerCase().indexOf("select");
        if (pos0 == -1) {
            throw new IllegalStateException("The given AWQL does not contains a \"select\" clause. The \"select\" clause is mandatory in AdHock reports!");
        } else {
            pos0 += "select".length();
            int pos1 = awql.toLowerCase().indexOf("from");
            if (pos1 == -1) {
                throw new IllegalStateException("The given AWQL does not contains a \"from\" clause. The \"from\" clause is mandatory in AdHock reports!");
            } else {
                fields = awql.substring(pos0, pos1).trim();
                if (fields.isEmpty()) {
                    throw new IllegalStateException("The given AWQL does not contains fields in the \"select\" clause!");
                } else {
                    buildFieldList();
                }
            }
        }
    }

    private void buildFieldList() {
        fieldList = new ArrayList();
        StringTokenizer st = new StringTokenizer(fields, ",");

        while (st.hasMoreTokens()) {
            String field = st.nextToken().trim();
            fieldList.add(field);
        }

    }

    public String getReportType() {
        return reportType;
    }

    public String getFieldsAsString() {
        return fields;
    }

    public List<String> getFieldsAsList() {
        return fieldList;
    }
}
