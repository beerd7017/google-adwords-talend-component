package com.adwords.google.desj;


import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ReportResultParser {
    private static final String DATE_DIMENSION = "Day";
    private List<String> requestedDimensionNames = new ArrayList();
    private List<String> requestedMetricNames = new ArrayList();
    private BufferedReader br = null;
    private boolean validFile = false;
    private String headerLine = null;
    private List<String> headers = null;
    private String currentLine = null;
    private int currentPlainRowIndex = 0;
    private int maxCountNormalizedValues = 0;
    private int currentNormalizedValueIndex = 0;
    private List<DimensionValue> currentResultRowDimensionValues;
    private Date currentDate;
    private boolean excludeDate = false;
    private List<MetricValue> currentResultRowMetricValues;
    private int countDimensions = 0;
    private String profileIdInfo = null;
    private String dimensionsInfo = null;
    private String metricsInfo = null;
    private String filtersInfo = null;
    private String segmentInfo = null;
    private String startDateInfo = null;
    private String endDateInfo = null;
    private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
    private boolean initialised = false;
    private boolean configurePositionByHeaderLine = false;
    private char[] data;
    private int lastPosDel = 0;
    private int lastDelimiterIndex = 0;
    private char[] delimiterChars = new char[]{','};
    private char[] enclosureChars = new char[]{'"'};
    private boolean allowEnclosureInText = true;

    public ReportResultParser() {
    }

    private static final char[] getChars(String s) {
        return s == null ? new char[0] : s.toCharArray();
    }

    private static final boolean startsWith(char[] data, char[] search, int startPos) {
        if (search.length != 0 && data.length != 0) {
            if (startPos >= 0 && startPos <= data.length - search.length) {
                int searchPos = 0;
                int count = search.length;
                int var5 = startPos;

                do {
                    --count;
                    if (count < 0) {
                        return true;
                    }
                } while (data[var5++] == search[searchPos++]);

                return false;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public void initialize(InputStream in) throws Exception {
        if (in == null) {
            this.initialised = false;
        } else {
            this.br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            this.processHeaderRows();
            this.initialised = true;
        }

    }

    public void initialize(String filePath) throws Exception {
        if (filePath != null && !filePath.trim().isEmpty()) {
            File f = new File(filePath.trim());
            if (!f.exists()) {
                throw new IllegalStateException("File " + f.getAbsolutePath() + " cannot be read!");
            } else {
                this.br = new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF-8"));
                this.processHeaderRows();
                this.initialised = true;
            }
        } else {
            throw new IllegalArgumentException("filePath cannot be null or empty.");
        }
    }

    public void close() {
        if (this.br != null) {
            try {
                this.br.close();
            } catch (IOException var2) {
                ;
            }
        }

    }

    public void setMetrics(String metrics) {
        if (metrics != null && !metrics.trim().isEmpty()) {
            this.requestedMetricNames = new ArrayList();
            String[] metricArray = metrics.split("[,;]");
            String[] arr$ = metricArray;
            int len$ = metricArray.length;

            for (int i$ = 0; i$ < len$; ++i$) {
                String metric = arr$[i$];
                this.requestedMetricNames.add(metric);
            }

        } else {
            throw new IllegalArgumentException("metrics cannot be null or empty");
        }
    }

    public void setFields(String fields) {
        this.setDimensions(fields);
    }

    public void setDimensions(String dimensions) {
        if (dimensions != null && !dimensions.trim().isEmpty()) {
            this.requestedDimensionNames = new ArrayList();
            String[] dimensionArray = dimensions.split("[,;]");
            String[] arr$ = dimensionArray;
            int len$ = dimensionArray.length;

            for (int i$ = 0; i$ < len$; ++i$) {
                String dimension = arr$[i$];
                this.requestedDimensionNames.add(dimension.trim());
            }

            this.countDimensions = this.requestedDimensionNames.size();
        } else {
            throw new IllegalArgumentException("dimensions cannot be null or empty");
        }
    }

    private void processHeaderRows() throws Exception {
        this.dimensionsInfo = null;
        this.metricsInfo = null;
        this.filtersInfo = null;
        this.segmentInfo = null;
        this.startDateInfo = null;
        this.endDateInfo = null;
        this.profileIdInfo = null;
        String line = this.br.readLine();
        if (line != null) {
            this.headerLine = line;
            this.validFile = line != null;
            if (this.headerLine != null) {
                this.setupHeaderPositions();
            }
        } else {
            this.validFile = false;
        }

    }

    private void setupHeaderPositions() throws Exception {
        int pos = 0;
        this.headers = new ArrayList();
        this.data = getChars(this.headerLine);
        this.lastPosDel = 0;
        this.lastDelimiterIndex = 0;

        while (true) {
            String column = this.extractDataAtDelimiter(pos);
            if (column == null || column.isEmpty()) {
                return;
            }

            this.headers.add(column.toLowerCase());
            ++pos;
        }
    }

    public boolean hasNextPlainRecord() throws IOException {
        if (this.initialised && this.validFile) {
            this.currentLine = this.br.readLine();
            if (this.currentLine != null) {
                return !this.currentLine.trim().isEmpty();
            } else {
                this.close();
                return false;
            }
        } else {
            this.close();
            return false;
        }
    }

    public List<String> getNextPlainRecord() throws Exception {
        if (this.currentLine == null) {
            throw new IllegalStateException("call hasNextPlainRecord before and check return true!");
        } else {
            List<String> record = new ArrayList();
            this.data = getChars(this.currentLine);
            this.lastPosDel = 0;
            this.lastDelimiterIndex = 0;

            String metric;
            int pos;
            for (int i = 0; i < this.requestedDimensionNames.size(); ++i) {
                metric = (String) this.requestedDimensionNames.get(i);
                if (this.configurePositionByHeaderLine) {
                    pos = this.headers.indexOf(metric.toLowerCase());
                    if (pos == -1) {
                        throw new Exception("Dimension " + metric + " not found in header line!");
                    }

                    record.add(this.extractDataAtDelimiter(pos));
                } else {
                    record.add(this.extractDataAtDelimiter(i));
                }
            }

            if (this.requestedMetricNames != null) {
                Iterator i$ = this.requestedMetricNames.iterator();

                while (i$.hasNext()) {
                    metric = (String) i$.next();
                    pos = this.headers.indexOf(metric.toLowerCase());
                    if (pos == -1) {
                        throw new Exception("Metric " + metric + " not found in header line!");
                    }

                    record.add(this.extractDataAtDelimiter(pos));
                }
            }

            ++this.currentPlainRowIndex;
            return record;
        }
    }

    private void setMaxCountNormalizedValues(int count) {
        if (count > this.maxCountNormalizedValues) {
            this.maxCountNormalizedValues = count;
        }

    }

    public DimensionValue getCurrentDimensionValue() {
        if (this.currentNormalizedValueIndex == 0) {
            throw new IllegalStateException("Call nextNormalizedRecord() at first!");
        } else {
            return this.currentNormalizedValueIndex <= this.currentResultRowDimensionValues.size() ? (DimensionValue) this.currentResultRowDimensionValues.get(this.currentNormalizedValueIndex - 1) : null;
        }
    }

    public Date getCurrentDate() {
        return this.currentDate;
    }

    public MetricValue getCurrentMetricValue() {
        if (this.currentNormalizedValueIndex == 0) {
            throw new IllegalStateException("Call nextNormalizedRecord() at first!");
        } else {
            return this.currentNormalizedValueIndex <= this.currentResultRowMetricValues.size() ? (MetricValue) this.currentResultRowMetricValues.get(this.currentNormalizedValueIndex - 1) : null;
        }
    }

    public boolean nextNormalizedRecord() throws Exception {
        if (!this.initialised) {
            return false;
        } else {
            if (this.maxCountNormalizedValues == 0 && this.hasNextPlainRecord()) {
                this.buildNormalizedRecords(this.getNextPlainRecord());
            }

            if (this.maxCountNormalizedValues > 0) {
                if (this.currentNormalizedValueIndex < this.maxCountNormalizedValues) {
                    ++this.currentNormalizedValueIndex;
                    return true;
                }

                if (this.currentNormalizedValueIndex == this.maxCountNormalizedValues && this.hasNextPlainRecord() && this.buildNormalizedRecords(this.getNextPlainRecord())) {
                    ++this.currentNormalizedValueIndex;
                    return true;
                }
            }

            return false;
        }
    }

    private boolean buildNormalizedRecords(List<String> oneRow) {
        this.maxCountNormalizedValues = 0;
        this.currentNormalizedValueIndex = 0;
        this.buildDimensionValues(oneRow);
        this.buildMetricValues(oneRow);
        return this.maxCountNormalizedValues > 0;
    }

    private List<DimensionValue> buildDimensionValues(List<String> oneRow) {
        int index = 0;
        this.currentDate = null;

        ArrayList oneRowDimensionValues;
        for (oneRowDimensionValues = new ArrayList(); index < this.requestedDimensionNames.size(); ++index) {
            DimensionValue dm = new DimensionValue();
            dm.name = (String) this.requestedDimensionNames.get(index);
            dm.value = (String) oneRow.get(index);
            dm.rowNum = this.currentPlainRowIndex;
            if (this.excludeDate && "Day".equalsIgnoreCase(dm.name.trim().toLowerCase())) {
                try {
                    if (dm.value != null) {
                        this.currentDate = this.dateFormatter.parse(dm.value);
                    }
                } catch (ParseException var6) {
                    throw new RuntimeException("Day value=" + dm.value + " cannot be parsed as Date.", var6);
                }
            } else {
                oneRowDimensionValues.add(dm);
            }
        }

        this.currentResultRowDimensionValues = oneRowDimensionValues;
        this.setMaxCountNormalizedValues(this.currentResultRowDimensionValues.size());
        return oneRowDimensionValues;
    }

    private List<MetricValue> buildMetricValues(List<String> oneRow) {
        int index = 0;

        ArrayList oneRowMetricValues;
        for (oneRowMetricValues = new ArrayList(); index < this.requestedMetricNames.size(); ++index) {
            MetricValue mv = new MetricValue();
            mv.name = (String) this.requestedMetricNames.get(index);
            mv.rowNum = this.currentPlainRowIndex;
            String valueStr = (String) oneRow.get(index + this.countDimensions);

            try {
                mv.value = Util.convertToDouble(valueStr, Locale.ENGLISH.toString());
                oneRowMetricValues.add(mv);
            } catch (Exception var7) {
                throw new IllegalStateException("Failed to build a double value for the metric:" + mv.name + " and value String:" + valueStr);
            }
        }

        this.currentResultRowMetricValues = oneRowMetricValues;
        this.setMaxCountNormalizedValues(this.currentResultRowMetricValues.size());
        return oneRowMetricValues;
    }

    private String extractDataAtDelimiter(int fieldNum) throws Exception {
        String value = null;
        if (fieldNum < this.lastDelimiterIndex) {
            throw new Exception("Current field index " + fieldNum + " is lower then last field index:" + this.lastDelimiterIndex);
        } else {
            int countDelimiters = this.lastDelimiterIndex;
            boolean inField = false;
            boolean atEnclosureStart = false;
            boolean atEnclosureStop = false;
            boolean atDelimiter = false;
            boolean useEnclosure = this.enclosureChars.length > 0;
            boolean fieldStartsWithEnclosure = false;
            boolean continueField = false;
            int currPos = this.lastPosDel;
            StringBuilder sb = new StringBuilder();

            while (currPos < this.data.length && countDelimiters <= fieldNum) {
                if (atEnclosureStart) {
                    atEnclosureStart = false;
                    fieldStartsWithEnclosure = true;
                    currPos += this.enclosureChars.length;
                    atEnclosureStop = startsWith(this.data, this.enclosureChars, currPos);
                    if (!atEnclosureStop) {
                        inField = true;
                    }
                } else if (atEnclosureStop) {
                    atEnclosureStop = false;
                    currPos += this.enclosureChars.length;
                    atDelimiter = startsWith(this.data, this.delimiterChars, currPos);
                    if (!atDelimiter && currPos < this.data.length) {
                        if (!this.allowEnclosureInText) {
                            throw new Exception("Delimiter after enclosure stop missing at position:" + currPos + " in field number:" + fieldNum);
                        }

                        inField = true;
                        continueField = true;
                        sb.append(this.enclosureChars);
                    }
                } else if (atDelimiter) {
                    ++countDelimiters;
                    fieldStartsWithEnclosure = false;
                    currPos += this.delimiterChars.length;
                    atDelimiter = startsWith(this.data, this.delimiterChars, currPos);
                    if (!atDelimiter) {
                        if (useEnclosure && currPos < this.data.length) {
                            atEnclosureStart = startsWith(this.data, this.enclosureChars, currPos);
                            if (!atEnclosureStart) {
                                inField = true;
                            }
                        } else {
                            inField = true;
                        }
                    }
                } else if (!inField) {
                    if (useEnclosure) {
                        atEnclosureStart = startsWith(this.data, this.enclosureChars, currPos);
                    }

                    atDelimiter = startsWith(this.data, this.delimiterChars, currPos);
                    if (!atEnclosureStart && !atDelimiter) {
                        inField = true;
                    }
                } else {
                    if (!continueField && countDelimiters == fieldNum) {
                        sb.setLength(0);
                    }

                    for (continueField = false; currPos < this.data.length; ++currPos) {
                        if (fieldStartsWithEnclosure) {
                            atEnclosureStop = startsWith(this.data, this.enclosureChars, currPos);
                            if (atEnclosureStop) {
                                break;
                            }
                        } else {
                            atDelimiter = startsWith(this.data, this.delimiterChars, currPos);
                            if (atDelimiter || atEnclosureStart) {
                                break;
                            }
                        }

                        if (countDelimiters == fieldNum) {
                            sb.append(this.data[currPos]);
                        }
                    }

                    inField = false;
                    if (countDelimiters == fieldNum) {
                        value = sb.toString();
                    }
                }
            }

            this.lastPosDel = currPos;
            this.lastDelimiterIndex = fieldNum + 1;
            return value;
        }
    }

    public String getProfileIdInfo() {
        return this.profileIdInfo;
    }

    public String getDimensionsInfo() {
        return this.dimensionsInfo;
    }

    public String getMetricsInfo() {
        return this.metricsInfo;
    }

    public String getFiltersInfo() {
        return this.filtersInfo;
    }

    public String getSegmentInfo() {
        return this.segmentInfo;
    }

    public String getStartDateInfo() {
        return this.startDateInfo;
    }

    public String getEndDateInfo() {
        return this.endDateInfo;
    }

    public boolean isExcludeDate() {
        return this.excludeDate;
    }

    public void setExcludeDate(boolean excludeDate) {
        this.excludeDate = excludeDate;
    }
}
