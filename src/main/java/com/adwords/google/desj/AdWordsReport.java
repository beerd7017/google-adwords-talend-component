package com.adwords.google.desj;


import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.adwords.lib.client.reporting.ReportingConfiguration;
import com.google.api.ads.adwords.lib.jaxb.v201710.*;
import com.google.api.ads.adwords.lib.utils.ReportDownloadResponse;
import com.google.api.ads.adwords.lib.utils.v201710.ReportDownloader;
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential.Builder;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Clock;
import com.google.api.client.util.store.FileDataStoreFactory;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;


public class AdWordsReport {
    private static final Map<String, AdWordsReport> clientCache = new HashMap();
    private static final String ADWORDS_SCOPE = "https://www.googleapis.com/auth/adwords";
    private final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    private final JsonFactory JSON_FACTORY = new JacksonFactory();
    private Logger logger = null;
    private File keyFile;
    private String clientSecretFile = null;
    private String clientId = null;
    private String clientSecret;
    private String serviceAccountIdEmail;
    private long timeMillisOffsetToPast = 10000L;
    private String userEmail = null;
    private AdWordsSession session = null;
    private boolean useServiceAccount = false;
    private boolean useClientId = false;
    private String clientCustomerId = null;
    private String refreshToken = null;
    private String userAgent = "com.adwords.google.desj";
    private String developerToken = null;
    private String adwordsPropertyFilePath = null;
    private boolean usePropertyFile = false;
    private String reportType = null;
    private String fields = null;
    private String startDateStr = null;
    private String endDateStr = null;
    private boolean useAWQL = false;
    private String awql = null;

    private String downloadDir = null;
    private String reportName = null;
    private String reportDownloadFilePath = null;
    private SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMdd");
    private boolean debug = true;
    private ReportDefinition reportDefinition;
    private boolean deliverTotalsDataset = false;
    private DownloadFormat downloadFormat;
    private int httpStatus;
    private ReportDownloadResponse response;
    private boolean sendReportAsAWQL;
    private String awqlWhereClause;
    private InputStream responseInputStream;

    public AdWordsReport() {
        downloadFormat = DownloadFormat.CSV;
        httpStatus = 0;
        response = null;
        sendReportAsAWQL = false;
        awqlWhereClause = null;
        responseInputStream = null;
    }

    public static void putIntoCache(String key, AdWordsReport gai) {
        clientCache.put(key, gai);
    }

    public static AdWordsReport getFromCache(String key) {
        AdWordsReport adWordsReport = (AdWordsReport) clientCache.get(key);
        if (adWordsReport != null) {
            adWordsReport.reset();
            return adWordsReport;
        } else {
            return null;
        }
    }

    public static boolean isEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }

    public static String unzip(String gzFilePath, boolean removeArchive) throws IOException {
        File archive = new File(gzFilePath);
        if (!archive.canRead()) {
            throw new IOException("Archive file: " + gzFilePath + " cannot be read or does not exist.");
        } else {
            String archiveFileName = archive.getName();
            String targetFileName = archiveFileName.substring(0, archiveFileName.length() - ".gz".length());
            File targetFile = new File(archive.getParent(), targetFileName);
            InputStream in = new GZIPInputStream(new FileInputStream(archive));
            FileOutputStream out = new FileOutputStream(targetFile);

            try {
                byte[] buffer = new byte[1024];

                for (int c = in.read(buffer); c > -1; c = in.read(buffer)) {
                    out.write(buffer, 0, c);
                }
            } finally {
                if (in != null) {
                    in.close();
                }

                if (out != null) {
                    out.close();
                }

            }

            if (removeArchive) {
                archive.delete();
            }

            return targetFile.getAbsolutePath();
        }
    }

    public void reset() {
        awql = null;
        reportType = null;
        fields = null;
        startDateStr = null;
        endDateStr = null;
        useAWQL = false;
        downloadDir = null;
        reportName = null;
        reportDownloadFilePath = null;
        reportDefinition = null;
        downloadFormat = DownloadFormat.CSV;
        httpStatus = 0;
        response = null;
        awqlWhereClause = null;
        responseInputStream = null;
    }

    private Credential authorizeWithServiceAccount() throws Exception {
        if (keyFile == null) {
            throw new Exception("KeyFile not set!");
        } else if (!keyFile.canRead()) {
            throw new IOException("keyFile:" + keyFile.getAbsolutePath() + " is not readable");
        } else if (serviceAccountIdEmail != null && !serviceAccountIdEmail.isEmpty()) {
            return (new Builder()).setTransport(HTTP_TRANSPORT).setJsonFactory(JSON_FACTORY).setServiceAccountId(serviceAccountIdEmail).setServiceAccountScopes(Arrays.asList("https://www.googleapis.com/auth/adwords")).setServiceAccountPrivateKeyFromP12File(keyFile).setServiceAccountUser(userEmail).setClock(new Clock() {
                public long currentTimeMillis() {
                    return System.currentTimeMillis() - AdWordsReport.this.timeMillisOffsetToPast;
                }
            }).build();
        } else {
            throw new Exception("account email cannot be null or empty");
        }
    }

    private Credential authorizeWithClientSecretAndRefreshToken() throws Exception {
        info("Authorise with Client-ID for installed application with existing refresh token ....");
        Credential oAuth2Credential = null;
        if (usePropertyFile) {
            info("... use property file: " + adwordsPropertyFilePath);
            oAuth2Credential = (new com.google.api.ads.common.lib.auth.OfflineCredentials.Builder()).forApi(Api.ADWORDS).fromFile(adwordsPropertyFilePath).build().generateCredential();
        } else {
            info("... set properties directly");
            if (clientId == null) {
                throw new IllegalStateException("Client-ID not set or null");
            }

            if (clientSecretFile == null) {
                throw new IllegalStateException("Client-ID not set or null");
            }

            oAuth2Credential = (new com.google.api.ads.common.lib.auth.OfflineCredentials.Builder()).forApi(Api.ADWORDS).withClientSecrets(clientId, clientSecret).withHttpTransport(HTTP_TRANSPORT).withRefreshToken(refreshToken).build().generateCredential();
        }

        return oAuth2Credential;
    }

    private Credential authorizeWithClientSecret() throws Exception {
        info("Authorise with Client-ID for installed application with using credential data store....");
        if (clientSecretFile == null) {
            throw new IllegalStateException("client secret file is not set");
        } else {
            File secretFile = new File(clientSecretFile);
            if (!secretFile.exists()) {
                throw new Exception("Client secret file:" + secretFile.getAbsolutePath() + " does not exists or is not readable.");
            } else {
                Reader reader = new FileReader(secretFile);
                GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, reader);

                try {
                    reader.close();
                } catch (Throwable var8) {
                    ;
                }

                if (!clientSecrets.getDetails().getClientId().startsWith("Enter") && !clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
                    String credentialDataStoreDir = secretFile.getParent() + "/" + clientSecrets.getDetails().getClientId() + "/";
                    File credentialDataStoreDirFile = new File(credentialDataStoreDir);
                    if (!credentialDataStoreDirFile.exists() && !credentialDataStoreDirFile.mkdirs()) {
                        throw new Exception("Credentedial data dir does not exists or cannot created:" + credentialDataStoreDir);
                    } else {
                        if (debug) {
                            info("Credential data store dir:" + credentialDataStoreDir);
                        }

                        FileDataStoreFactory fdsf = new FileDataStoreFactory(credentialDataStoreDirFile);
                        GoogleAuthorizationCodeFlow flow = (new com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, Arrays.asList("https://www.googleapis.com/auth/adwords"))).setDataStoreFactory(fdsf).setClock(new Clock() {
                            public long currentTimeMillis() {
                                return System.currentTimeMillis() - AdWordsReport.this.timeMillisOffsetToPast;
                            }
                        }).build();
                        return (new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver())).authorize(userEmail);
                    }
                } else {
                    throw new Exception("The client secret file does not contains the credentials!");
                }
            }
        }
    }

    public void initializeAdWordsSession() throws Exception {
        Credential oAuth2Credential = null;
        if (useServiceAccount) {
            oAuth2Credential = authorizeWithServiceAccount();
        } else {
            if (!useClientId) {
                throw new IllegalStateException("Not authorized. Please choose an authorization method!");
            }

            if (refreshToken == null && !usePropertyFile) {
                oAuth2Credential = authorizeWithClientSecret();
            } else {
                oAuth2Credential = authorizeWithClientSecretAndRefreshToken();
            }
        }

        if (oAuth2Credential == null) {
            error("Authentication failed. Check the Exception thrown before.", (Exception) null);
        } else {
            oAuth2Credential.refreshToken();
            if (usePropertyFile) {
                session = (new com.google.api.ads.adwords.lib.client.AdWordsSession.Builder()).fromFile(adwordsPropertyFilePath).withOAuth2Credential(oAuth2Credential).build();
            } else {
                if (clientCustomerId == null) {
                    throw new IllegalStateException("clientCustomerId mus be set");
                }

                session = (new com.google.api.ads.adwords.lib.client.AdWordsSession.Builder()).withClientCustomerId(clientCustomerId).withDeveloperToken(developerToken).withUserAgent(userAgent).withOAuth2Credential(oAuth2Credential).build();
            }

        }
    }

    public void executeQuery() throws Exception {
        ReportingConfiguration reportingConfiguration = (new com.google.api.ads.adwords.lib.client.reporting.ReportingConfiguration.Builder()).skipReportHeader(true).skipReportSummary(!deliverTotalsDataset).build();
        int reportDownloadTimeout = 3000;
        session.setReportingConfiguration(reportingConfiguration);
        session.setValidateOnly(true);
        ReportDownloader downloader = new ReportDownloader(session);
        downloader.setReportDownloadTimeout(reportDownloadTimeout);
        if (sendReportAsAWQL && !useAWQL) {
            if (fields == null) {
                throw new IllegalStateException("No fields has been set!");
            }
            if (reportType == null) {
                throw new IllegalStateException("No reportType has been set!");
            }
            buildAWQLFromReportDefinition();
        }

        if (!sendReportAsAWQL && !useAWQL) {
            setupReportName();
            setupReportDefinition();
            response = downloader.downloadReport(reportDefinition);
        } else {
            if (awql == null) {
                throw new IllegalStateException("No AWQL has been set!");
            }
            if (!sendReportAsAWQL) {
                AWQLParser parser = new AWQLParser();
                parser.parse(awql);
                reportType = parser.getReportType();
                fields = parser.getFieldsAsString();
            }
            String awqlRequestFormat = AWQLParser.buildRequestFormat(awql);
            if (debug) {
                info("AWQL request formatted:" + awqlRequestFormat);
            }
            setupReportName();
            response = downloader.downloadReport(awqlRequestFormat, downloadFormat);
        }
        responseInputStream = response.getInputStream();
    }

    public void downloadAsFile() throws Exception {
        buildDownloadFile();
        info("Download report to: " + reportDownloadFilePath);
        httpStatus = response.getHttpStatus();
        response.saveToFile(reportDownloadFilePath);
        info("Finished.");
    }

    private File buildDownloadFile() {
        if (reportDownloadFilePath != null) {
            File df = new File(reportDownloadFilePath);
            if (!df.getParentFile().exists()) {
                df.getParentFile().mkdirs();
            }

            return df;
        } else if (downloadDir == null) {
            throw new IllegalStateException("Download dir not set!");
        } else if (reportType == null) {
            throw new IllegalStateException("Report-Type not set!");
        } else if (reportName == null) {
            throw new IllegalStateException("Report-Name not set!");
        } else if (downloadFormat == null) {
            throw new IllegalStateException("Download format not set!");
        } else {
            String fileExtension = null;
            if (downloadFormat == DownloadFormat.CSV) {
                fileExtension = ".csv";
            } else if (downloadFormat == DownloadFormat.CSVFOREXCEL) {
                fileExtension = ".bom.csv";
            } else if (downloadFormat == DownloadFormat.TSV) {
                fileExtension = ".tsv";
            } else if (downloadFormat == DownloadFormat.XML) {
                fileExtension = ".xml";
            } else if (downloadFormat == DownloadFormat.GZIPPED_CSV) {
                fileExtension = ".csv.gz";
            } else if (downloadFormat == DownloadFormat.GZIPPED_XML) {
                fileExtension = ".xml.gz";
            }

            File df = new File(downloadDir, reportName + fileExtension);
            if (!df.getParentFile().exists()) {
                df.getParentFile().mkdirs();
            }

            reportDownloadFilePath = df.getAbsolutePath();
            return df;
        }
    }

    private void buildAWQLFromReportDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        sb.append(fields);
        sb.append(" from ");
        sb.append(reportType);
        if (awqlWhereClause != null) {
            sb.append(" where ");
            sb.append(awqlWhereClause);
        }

        sb.append(" during ");
        sb.append(startDateStr);
        sb.append(",");
        sb.append(endDateStr);
        awql = sb.toString();
    }

    private ReportDefinition setupReportDefinition() {
        if (isEmpty(startDateStr)) {
            throw new IllegalStateException("Start date is not set");
        } else if (isEmpty(endDateStr)) {
            throw new IllegalStateException("End date is not set");
        } else {
            Selector selector = new Selector();
            selector.getFields().addAll(buildFieldList());
            DateRange dr = new DateRange();
            dr.setMin(startDateStr);
            dr.setMax(endDateStr);
            selector.setDateRange(dr);
            reportDefinition = new ReportDefinition();
            reportDefinition.setSelector(selector);
            reportDefinition.setReportType(getReportDefinitionType());
            reportDefinition.setReportName(reportName);
            reportDefinition.setDateRangeType(ReportDefinitionDateRangeType.fromValue("CUSTOM_DATE"));
            reportDefinition.setDownloadFormat(downloadFormat);
            return reportDefinition;
        }
    }

    private String setupReportName() {
        if (reportType == null) {
            throw new IllegalStateException("reportType cannot be null");
        } else if (reportName != null) {
            return reportName;
        } else {
            if (awql != null) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS");
                reportName = reportType + "#" + sdf.format(new Date());
            } else {
                reportName = reportType + "#" + startDateStr + "-" + endDateStr;
            }

            return reportName;
        }
    }

    private ReportDefinitionReportType getReportDefinitionType() {
        if (reportType == null) {
            throw new IllegalStateException("The report-type must be set!");
        } else {
            return ReportDefinitionReportType.fromValue(reportType);
        }
    }

    private List<String> buildFieldList() {
        if (fields == null) {
            throw new IllegalStateException("Fields cannot be empty. Please specify the report fields!");
        } else {
            List<String> fieldList = new ArrayList();
            StringTokenizer st = new StringTokenizer(fields, ",;|");

            while (st.hasMoreTokens()) {
                String field = st.nextToken().trim();
                fieldList.add(field);
            }

            return fieldList;
        }
    }

    public void setStartDate(String startDate) {
        if (!isEmpty(startDate)) {
            startDateStr = checkDate(startDate);
        }

    }

    public void setStartDate(Date startDate) {
        if (startDate != null) {
            startDateStr = sdfDate.format(startDate);
        }

    }

    public void setEndDate(String endDate) {
        if (!isEmpty(endDate)) {
            endDateStr = checkDate(endDate);
        }

    }

    public void setEndDate(Date endDate) {
        if (endDate != null) {
            endDateStr = sdfDate.format(endDate);
        }

    }

    private String checkDate(String dateStr) {
        if (isEmpty(dateStr)) {
            throw new IllegalArgumentException("the given date is empty or null");
        } else {
            dateStr = dateStr.replace("-", "");
            return dateStr;
        }
    }

    public String getReportDownloadFilePath() {
        return reportDownloadFilePath;
    }

    public void setReportDownloadFilePath(String reportDownloadFilePath) {
        if (!isEmpty(reportDownloadFilePath)) {
            reportDownloadFilePath = reportDownloadFilePath;
        }

    }

    public void setDeveloperToken(String developerToken) {
        if (!isEmpty(developerToken)) {
            developerToken = developerToken;
        }

    }

    public void setUserAgent(String userAgent) {
        if (!isEmpty(userAgent)) {
            userAgent = userAgent;
        }

    }

    public void setClientCustomerId(String clientCustomerId) {
        if (!isEmpty(clientCustomerId)) {
            clientCustomerId = clientCustomerId;
        }

    }

    public void setClientId(String clientId) {
        if (!isEmpty(clientId)) {
            clientId = clientId;
        }

    }

    public void setClientSecretFile(String clientSecretFile) {
        if (!isEmpty(clientSecretFile)) {
            clientSecretFile = clientSecretFile;
        }

    }

    public void setKeyFile(String keyFileStr) {
        if (!isEmpty(keyFileStr)) {
            File f = new File(keyFileStr);
            if (!f.canRead()) {
                throw new IllegalArgumentException("Key file:" + keyFileStr + " cannot be read!");
            }

            keyFile = f;
        }

    }

    public void setServiceAccountIdEmail(String accountEmail) {
        if (!isEmpty(accountEmail)) {
            serviceAccountIdEmail = accountEmail;
        }

    }

    public void setUserEmail(String adWordsAccountEmail) {
        if (!isEmpty(adWordsAccountEmail)) {
            userEmail = adWordsAccountEmail;
        }

    }

    public void setUseServiceAccount(boolean useServiceAccount) {
        useServiceAccount = useServiceAccount;
    }

    public void setAdwordsPropertyFilePath(String adwordsPropertyFilePath) {
        if (!isEmpty(adwordsPropertyFilePath)) {
            adwordsPropertyFilePath = adwordsPropertyFilePath;
        }

    }

    public void setUsePropertyFile(boolean usePropertyFile) {
        usePropertyFile = usePropertyFile;
    }

    public void setAwql(String awql) {
        if (!isEmpty(awql)) {
            awql = awql.trim();
        }

    }

    public void setDownloadDir(String downloadDir) {
        if (!isEmpty(downloadDir)) {
            downloadDir = downloadDir;
        }

    }

    public void setUseClientId(boolean useClientId) {
        useClientId = useClientId;
    }

    public void setRefreshToken(String refreshToken) {
        if (!isEmpty(refreshToken)) {
            refreshToken = refreshToken;
        }

    }

    public void setClientSecret(String clientSecret) {
        if (!isEmpty(clientSecret)) {
            clientSecret = clientSecret;
        }

    }

    public String getReportName() {
        return reportName;
    }

    public void setReportName(String reportName) {
        if (!isEmpty(reportName)) {
            reportName = reportName;
        }

    }

    public void deliverTotalsDataset(boolean deliverTotalsDataset) {
        deliverTotalsDataset = deliverTotalsDataset;
    }

    public void setTimeMillisOffsetToPast(Long timeMillisOffsetToPast) {
        if (timeMillisOffsetToPast != null) {
            timeMillisOffsetToPast = timeMillisOffsetToPast;
        }

    }

    public void setTimeMillisOffsetToPast(Integer timeMillisOffsetToPast) {
        if (timeMillisOffsetToPast != null) {
            this.timeMillisOffsetToPast = timeMillisOffsetToPast.longValue();
        }

    }

    public String getReportType() {
        return reportType;
    }

    public void setReportType(String reportType) {
        if (!isEmpty(reportType)) {
            reportType = reportType;
        }

    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        debug = debug;
    }

    public void setDownloadFormat(String format) {
        if ("CSV".equalsIgnoreCase(format)) {
            downloadFormat = DownloadFormat.CSV;
        } else if ("XML".equalsIgnoreCase(format)) {
            downloadFormat = DownloadFormat.XML;
        } else if ("TSV".equalsIgnoreCase(format)) {
            downloadFormat = DownloadFormat.TSV;
        } else if ("GZIPPED_CSV".equalsIgnoreCase(format)) {
            downloadFormat = DownloadFormat.GZIPPED_CSV;
        } else if ("GZIPPED_XML".equalsIgnoreCase(format)) {
            downloadFormat = DownloadFormat.GZIPPED_XML;
        } else {
            if (!"CSVFOREXCEL".equalsIgnoreCase(format)) {
                throw new IllegalArgumentException("Download format:" + format + " is not supported!");
            }

            downloadFormat = DownloadFormat.CSVFOREXCEL;
        }

    }

    public boolean downloadIsAnArchive() {
        return downloadFormat == DownloadFormat.GZIPPED_CSV || downloadFormat == DownloadFormat.GZIPPED_XML;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public void info(String message) {
        if (logger != null) {
            logger.info(message);
        } else {
            System.out.println("INFO:" + message);
        }

    }

    public void debug(String message) {
        if (logger != null) {
            logger.debug(message);
        } else {
            System.out.println("DEBUG:" + message);
        }

    }

    public void warn(String message) {
        if (logger != null) {
            logger.warn(message);
        } else {
            System.err.println("WARN:" + message);
        }

    }

    public void error(String message, Exception e) {
        if (logger != null) {
            logger.error(message, e);
        } else {
            System.err.println("ERROR:" + message);
        }

    }

    public void setLogger(Logger logger) {
        logger = logger;
    }

    public void sendReportAsAWQL(boolean createAWQL) {
        sendReportAsAWQL = createAWQL;
    }

    public void setAWQLWhereClause(String awqlWhereClause) {
        if (!isEmpty(awqlWhereClause)) {
            awqlWhereClause = awqlWhereClause;
        }

    }

    public boolean isUseAWQL() {
        return useAWQL;
    }

    public void setUseAWQL(boolean useAWQL) {
        useAWQL = useAWQL;
    }

    public InputStream getResponseInputStream() {
        return responseInputStream;
    }

    public String getFields() {
        return fields;
    }

    public void setFields(String fields) {
        if (!isEmpty(fields)) {
            fields = fields;
        }

    }
}
