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
        this.awql = awql.trim();
        this.findFields();
        this.findReportType();
        return this;
    }

    private void findReportType() {
        int pos0 = this.awql.toLowerCase().indexOf("from");
        if (pos0 == -1) {
            throw new IllegalStateException("The given AWQL does not contains a \"from\" clause. The \"from\" clause is mandatory in AdHock reports!");
        } else {
            pos0 += "from".length();
            int pos1 = this.awql.toLowerCase().indexOf("where");
            if (pos1 == -1) {
                pos1 = this.awql.toLowerCase().indexOf("during");
            }

            if (pos1 == -1) {
                throw new IllegalStateException("The given AWQL does not contains a \"during\" clause. The \"during\" clause is mandatory in AdHock reports!");
            } else {
                this.reportType = this.awql.substring(pos0, pos1).trim();
                if (this.reportType.isEmpty()) {
                    throw new IllegalStateException("The given AWQL does not contains a report-type in the \"from\" clause!");
                }
            }
        }
    }

    private void findFields() {
        int pos0 = this.awql.toLowerCase().indexOf("select");
        if (pos0 == -1) {
            throw new IllegalStateException("The given AWQL does not contains a \"select\" clause. The \"select\" clause is mandatory in AdHock reports!");
        } else {
            pos0 += "select".length();
            int pos1 = this.awql.toLowerCase().indexOf("from");
            if (pos1 == -1) {
                throw new IllegalStateException("The given AWQL does not contains a \"from\" clause. The \"from\" clause is mandatory in AdHock reports!");
            } else {
                this.fields = this.awql.substring(pos0, pos1).trim();
                if (this.fields.isEmpty()) {
                    throw new IllegalStateException("The given AWQL does not contains fields in the \"select\" clause!");
                } else {
                    this.buildFieldList();
                }
            }
        }
    }

    private void buildFieldList() {
        this.fieldList = new ArrayList();
        StringTokenizer st = new StringTokenizer(this.fields, ",");

        while (st.hasMoreTokens()) {
            String field = st.nextToken().trim();
            this.fieldList.add(field);
        }

    }

    public String getReportType() {
        return this.reportType;
    }

    public String getFieldsAsString() {
        return this.fields;
    }

    public List<String> getFieldsAsList() {
        return this.fieldList;
    }
}
