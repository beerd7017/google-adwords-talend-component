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
            initialised = false;
        } else {
            br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            processHeaderRows();
            initialised = true;
        }

    }

    public void initialize(String filePath) throws Exception {
        if (filePath != null && !filePath.trim().isEmpty()) {
            File f = new File(filePath.trim());
            if (!f.exists()) {
                throw new IllegalStateException("File " + f.getAbsolutePath() + " cannot be read!");
            } else {
                br = new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF-8"));
                processHeaderRows();
                initialised = true;
            }
        } else {
            throw new IllegalArgumentException("filePath cannot be null or empty.");
        }
    }

    public void close() {
        if (br != null) {
            try {
                br.close();
            } catch (IOException var2) {
                ;
            }
        }

    }

    public void setMetrics(String metrics) {
        if (metrics != null && !metrics.trim().isEmpty()) {
            requestedMetricNames = new ArrayList();
            String[] metricArray = metrics.split("[,;]");
            String[] arr$ = metricArray;
            int len$ = metricArray.length;

            for (int i$ = 0; i$ < len$; ++i$) {
                String metric = arr$[i$];
                requestedMetricNames.add(metric);
            }

        } else {
            throw new IllegalArgumentException("metrics cannot be null or empty");
        }
    }

    public void setFields(String fields) {
        setDimensions(fields);
    }

    public void setDimensions(String dimensions) {
        if (dimensions != null && !dimensions.trim().isEmpty()) {
            requestedDimensionNames = new ArrayList();
            String[] dimensionArray = dimensions.split("[,;]");
            String[] arr$ = dimensionArray;
            int len$ = dimensionArray.length;

            for (int i$ = 0; i$ < len$; ++i$) {
                String dimension = arr$[i$];
                requestedDimensionNames.add(dimension.trim());
            }

            countDimensions = requestedDimensionNames.size();
        } else {
            throw new IllegalArgumentException("dimensions cannot be null or empty");
        }
    }

    private void processHeaderRows() throws Exception {
        dimensionsInfo = null;
        metricsInfo = null;
        filtersInfo = null;
        segmentInfo = null;
        startDateInfo = null;
        endDateInfo = null;
        profileIdInfo = null;
        String line = br.readLine();
        if (line != null) {
            headerLine = line;
            validFile = line != null;
            if (headerLine != null) {
                setupHeaderPositions();
            }
        } else {
            validFile = false;
        }

    }

    private void setupHeaderPositions() throws Exception {
        int pos = 0;
        headers = new ArrayList();
        data = getChars(headerLine);
        lastPosDel = 0;
        lastDelimiterIndex = 0;

        while (true) {
            String column = extractDataAtDelimiter(pos);
            if (column == null || column.isEmpty()) {
                return;
            }

            headers.add(column.toLowerCase());
            ++pos;
        }
    }

    public boolean hasNextPlainRecord() throws IOException {
        if (initialised && validFile) {
            currentLine = br.readLine();
            if (currentLine != null) {
                return !currentLine.trim().isEmpty();
            } else {
                close();
                return false;
            }
        } else {
            close();
            return false;
        }
    }

    public List<String> getNextPlainRecord() throws Exception {
        if (currentLine == null) {
            throw new IllegalStateException("call hasNextPlainRecord before and check return true!");
        } else {
            List<String> record = new ArrayList();
            data = getChars(currentLine);
            lastPosDel = 0;
            lastDelimiterIndex = 0;

            String metric;
            int pos;
            for (int i = 0; i < requestedDimensionNames.size(); ++i) {
                metric = (String) requestedDimensionNames.get(i);
                if (configurePositionByHeaderLine) {
                    pos = headers.indexOf(metric.toLowerCase());
                    if (pos == -1) {
                        throw new Exception("Dimension " + metric + " not found in header line!");
                    }

                    record.add(extractDataAtDelimiter(pos));
                } else {
                    record.add(extractDataAtDelimiter(i));
                }
            }

            if (requestedMetricNames != null) {
                Iterator i$ = requestedMetricNames.iterator();

                while (i$.hasNext()) {
                    metric = (String) i$.next();
                    pos = headers.indexOf(metric.toLowerCase());
                    if (pos == -1) {
                        throw new Exception("Metric " + metric + " not found in header line!");
                    }

                    record.add(extractDataAtDelimiter(pos));
                }
            }

            ++currentPlainRowIndex;
            return record;
        }
    }

    private void setMaxCountNormalizedValues(int count) {
        if (count > maxCountNormalizedValues) {
            maxCountNormalizedValues = count;
        }

    }

    public DimensionValue getCurrentDimensionValue() {
        if (currentNormalizedValueIndex == 0) {
            throw new IllegalStateException("Call nextNormalizedRecord() at first!");
        } else {
            return currentNormalizedValueIndex <= currentResultRowDimensionValues.size() ? (DimensionValue) currentResultRowDimensionValues.get(currentNormalizedValueIndex - 1) : null;
        }
    }

    public Date getCurrentDate() {
        return currentDate;
    }

    public MetricValue getCurrentMetricValue() {
        if (currentNormalizedValueIndex == 0) {
            throw new IllegalStateException("Call nextNormalizedRecord() at first!");
        } else {
            return currentNormalizedValueIndex <= currentResultRowMetricValues.size() ? (MetricValue) currentResultRowMetricValues.get(currentNormalizedValueIndex - 1) : null;
        }
    }

    public boolean nextNormalizedRecord() throws Exception {
        if (!initialised) {
            return false;
        } else {
            if (maxCountNormalizedValues == 0 && hasNextPlainRecord()) {
                buildNormalizedRecords(getNextPlainRecord());
            }

            if (maxCountNormalizedValues > 0) {
                if (currentNormalizedValueIndex < maxCountNormalizedValues) {
                    ++currentNormalizedValueIndex;
                    return true;
                }

                if (currentNormalizedValueIndex == maxCountNormalizedValues && hasNextPlainRecord() && buildNormalizedRecords(getNextPlainRecord())) {
                    ++currentNormalizedValueIndex;
                    return true;
                }
            }

            return false;
        }
    }

    private boolean buildNormalizedRecords(List<String> oneRow) {
        maxCountNormalizedValues = 0;
        currentNormalizedValueIndex = 0;
        buildDimensionValues(oneRow);
        buildMetricValues(oneRow);
        return maxCountNormalizedValues > 0;
    }

    private List<DimensionValue> buildDimensionValues(List<String> oneRow) {
        int index = 0;
        currentDate = null;

        ArrayList oneRowDimensionValues;
        for (oneRowDimensionValues = new ArrayList(); index < requestedDimensionNames.size(); ++index) {
            DimensionValue dm = new DimensionValue();
            dm.name = (String) requestedDimensionNames.get(index);
            dm.value = (String) oneRow.get(index);
            dm.rowNum = currentPlainRowIndex;
            if (excludeDate && "Day".equalsIgnoreCase(dm.name.trim().toLowerCase())) {
                try {
                    if (dm.value != null) {
                        currentDate = dateFormatter.parse(dm.value);
                    }
                } catch (ParseException var6) {
                    throw new RuntimeException("Day value=" + dm.value + " cannot be parsed as Date.", var6);
                }
            } else {
                oneRowDimensionValues.add(dm);
            }
        }

        currentResultRowDimensionValues = oneRowDimensionValues;
        setMaxCountNormalizedValues(currentResultRowDimensionValues.size());
        return oneRowDimensionValues;
    }

    private List<MetricValue> buildMetricValues(List<String> oneRow) {
        int index = 0;

        ArrayList oneRowMetricValues;
        for (oneRowMetricValues = new ArrayList(); index < requestedMetricNames.size(); ++index) {
            MetricValue mv = new MetricValue();
            mv.name = (String) requestedMetricNames.get(index);
            mv.rowNum = currentPlainRowIndex;
            String valueStr = (String) oneRow.get(index + countDimensions);

            try {
                mv.value = Util.convertToDouble(valueStr, Locale.ENGLISH.toString());
                oneRowMetricValues.add(mv);
            } catch (Exception var7) {
                throw new IllegalStateException("Failed to build a double value for the metric:" + mv.name + " and value String:" + valueStr);
            }
        }

        currentResultRowMetricValues = oneRowMetricValues;
        setMaxCountNormalizedValues(currentResultRowMetricValues.size());
        return oneRowMetricValues;
    }

    private String extractDataAtDelimiter(int fieldNum) throws Exception {
        String value = null;
        if (fieldNum < lastDelimiterIndex) {
            throw new Exception("Current field index " + fieldNum + " is lower then last field index:" + lastDelimiterIndex);
        } else {
            int countDelimiters = lastDelimiterIndex;
            boolean inField = false;
            boolean atEnclosureStart = false;
            boolean atEnclosureStop = false;
            boolean atDelimiter = false;
            boolean useEnclosure = enclosureChars.length > 0;
            boolean fieldStartsWithEnclosure = false;
            boolean continueField = false;
            int currPos = lastPosDel;
            StringBuilder sb = new StringBuilder();

            while (currPos < data.length && countDelimiters <= fieldNum) {
                if (atEnclosureStart) {
                    atEnclosureStart = false;
                    fieldStartsWithEnclosure = true;
                    currPos += enclosureChars.length;
                    atEnclosureStop = startsWith(data, enclosureChars, currPos);
                    if (!atEnclosureStop) {
                        inField = true;
                    }
                } else if (atEnclosureStop) {
                    atEnclosureStop = false;
                    currPos += enclosureChars.length;
                    atDelimiter = startsWith(data, delimiterChars, currPos);
                    if (!atDelimiter && currPos < data.length) {
                        if (!allowEnclosureInText) {
                            throw new Exception("Delimiter after enclosure stop missing at position:" + currPos + " in field number:" + fieldNum);
                        }

                        inField = true;
                        continueField = true;
                        sb.append(enclosureChars);
                    }
                } else if (atDelimiter) {
                    ++countDelimiters;
                    fieldStartsWithEnclosure = false;
                    currPos += delimiterChars.length;
                    atDelimiter = startsWith(data, delimiterChars, currPos);
                    if (!atDelimiter) {
                        if (useEnclosure && currPos < data.length) {
                            atEnclosureStart = startsWith(data, enclosureChars, currPos);
                            if (!atEnclosureStart) {
                                inField = true;
                            }
                        } else {
                            inField = true;
                        }
                    }
                } else if (!inField) {
                    if (useEnclosure) {
                        atEnclosureStart = startsWith(data, enclosureChars, currPos);
                    }

                    atDelimiter = startsWith(data, delimiterChars, currPos);
                    if (!atEnclosureStart && !atDelimiter) {
                        inField = true;
                    }
                } else {
                    if (!continueField && countDelimiters == fieldNum) {
                        sb.setLength(0);
                    }

                    for (continueField = false; currPos < data.length; ++currPos) {
                        if (fieldStartsWithEnclosure) {
                            atEnclosureStop = startsWith(data, enclosureChars, currPos);
                            if (atEnclosureStop) {
                                break;
                            }
                        } else {
                            atDelimiter = startsWith(data, delimiterChars, currPos);
                            if (atDelimiter || atEnclosureStart) {
                                break;
                            }
                        }

                        if (countDelimiters == fieldNum) {
                            sb.append(data[currPos]);
                        }
                    }

                    inField = false;
                    if (countDelimiters == fieldNum) {
                        value = sb.toString();
                    }
                }
            }

            lastPosDel = currPos;
            lastDelimiterIndex = fieldNum + 1;
            return value;
        }
    }

    public String getProfileIdInfo() {
        return profileIdInfo;
    }

    public String getDimensionsInfo() {
        return dimensionsInfo;
    }

    public String getMetricsInfo() {
        return metricsInfo;
    }

    public String getFiltersInfo() {
        return filtersInfo;
    }

    public String getSegmentInfo() {
        return segmentInfo;
    }

    public String getStartDateInfo() {
        return startDateInfo;
    }

    public String getEndDateInfo() {
        return endDateInfo;
    }

    public boolean isExcludeDate() {
        return excludeDate;
    }

    public void setExcludeDate(boolean excludeDate) {
        excludeDate = excludeDate;
    }
}
