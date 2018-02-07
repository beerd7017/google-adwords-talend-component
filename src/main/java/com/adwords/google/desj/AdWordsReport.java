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

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import java.util.zip.GZIPInputStream;
import org.apache.log4j.Logger;


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
    private int reportDownloadTimeout = 3000;
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
        this.downloadFormat = DownloadFormat.CSV;
        this.httpStatus = 0;
        this.response = null;
        this.sendReportAsAWQL = false;
        this.awqlWhereClause = null;
        this.responseInputStream = null;
    }

    public static void putIntoCache(String key, AdWordsReport gai) {
        clientCache.put(key, gai);
    }

    public static AdWordsReport getFromCache(String key) {
        AdWordsReport adr = (AdWordsReport) clientCache.get(key);
        if (adr != null) {
            adr.reset();
            return adr;
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
        this.awql = null;
        this.reportType = null;
        this.fields = null;
        this.startDateStr = null;
        this.endDateStr = null;
        this.useAWQL = false;
        this.downloadDir = null;
        this.reportName = null;
        this.reportDownloadFilePath = null;
        this.reportDefinition = null;
        this.downloadFormat = DownloadFormat.CSV;
        this.httpStatus = 0;
        this.response = null;
        this.awqlWhereClause = null;
        this.responseInputStream = null;
    }

    private Credential authorizeWithServiceAccount() throws Exception {
        if (this.keyFile == null) {
            throw new Exception("KeyFile not set!");
        } else if (!this.keyFile.canRead()) {
            throw new IOException("keyFile:" + this.keyFile.getAbsolutePath() + " is not readable");
        } else if (this.serviceAccountIdEmail != null && !this.serviceAccountIdEmail.isEmpty()) {
            return (new Builder()).setTransport(this.HTTP_TRANSPORT).setJsonFactory(this.JSON_FACTORY).setServiceAccountId(this.serviceAccountIdEmail).setServiceAccountScopes(Arrays.asList("https://www.googleapis.com/auth/adwords")).setServiceAccountPrivateKeyFromP12File(this.keyFile).setServiceAccountUser(this.userEmail).setClock(new Clock() {
                public long currentTimeMillis() {
                    return System.currentTimeMillis() - AdWordsReport.this.timeMillisOffsetToPast;
                }
            }).build();
        } else {
            throw new Exception("account email cannot be null or empty");
        }
    }

    private Credential authorizeWithClientSecretAndRefreshToken() throws Exception {
        this.info("Authorise with Client-ID for installed application with existing refresh token ....");
        Credential oAuth2Credential = null;
        if (this.usePropertyFile) {
            this.info("... use property file: " + this.adwordsPropertyFilePath);
            oAuth2Credential = (new com.google.api.ads.common.lib.auth.OfflineCredentials.Builder()).forApi(Api.ADWORDS).fromFile(this.adwordsPropertyFilePath).build().generateCredential();
        } else {
            this.info("... set properties directly");
            if (this.clientId == null) {
                throw new IllegalStateException("Client-ID not set or null");
            }

            if (this.clientSecretFile == null) {
                throw new IllegalStateException("Client-ID not set or null");
            }

            oAuth2Credential = (new com.google.api.ads.common.lib.auth.OfflineCredentials.Builder()).forApi(Api.ADWORDS).withClientSecrets(this.clientId, this.clientSecret).withHttpTransport(this.HTTP_TRANSPORT).withRefreshToken(this.refreshToken).build().generateCredential();
        }

        return oAuth2Credential;
    }

    private Credential authorizeWithClientSecret() throws Exception {
        this.info("Authorise with Client-ID for installed application with using credential data store....");
        if (this.clientSecretFile == null) {
            throw new IllegalStateException("client secret file is not set");
        } else {
            File secretFile = new File(this.clientSecretFile);
            if (!secretFile.exists()) {
                throw new Exception("Client secret file:" + secretFile.getAbsolutePath() + " does not exists or is not readable.");
            } else {
                Reader reader = new FileReader(secretFile);
                GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(this.JSON_FACTORY, reader);

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
                        if (this.debug) {
                            this.info("Credential data store dir:" + credentialDataStoreDir);
                        }

                        FileDataStoreFactory fdsf = new FileDataStoreFactory(credentialDataStoreDirFile);
                        GoogleAuthorizationCodeFlow flow = (new com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow.Builder(this.HTTP_TRANSPORT, this.JSON_FACTORY, clientSecrets, Arrays.asList("https://www.googleapis.com/auth/adwords"))).setDataStoreFactory(fdsf).setClock(new Clock() {
                            public long currentTimeMillis() {
                                return System.currentTimeMillis() - AdWordsReport.this.timeMillisOffsetToPast;
                            }
                        }).build();
                        return (new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver())).authorize(this.userEmail);
                    }
                } else {
                    throw new Exception("The client secret file does not contains the credentials!");
                }
            }
        }
    }

    public void initializeAdWordsSession() throws Exception {
        Credential oAuth2Credential = null;
        if (this.useServiceAccount) {
            oAuth2Credential = this.authorizeWithServiceAccount();
        } else {
            if (!this.useClientId) {
                throw new IllegalStateException("Not authorized. Please choose an authorization method!");
            }

            if (this.refreshToken == null && !this.usePropertyFile) {
                oAuth2Credential = this.authorizeWithClientSecret();
            } else {
                oAuth2Credential = this.authorizeWithClientSecretAndRefreshToken();
            }
        }

        if (oAuth2Credential == null) {
            this.error("Authentication failed. Check the Exception thrown before.", (Exception) null);
        } else {
            oAuth2Credential.refreshToken();
            if (this.usePropertyFile) {
                this.session = (new com.google.api.ads.adwords.lib.client.AdWordsSession.Builder()).fromFile(this.adwordsPropertyFilePath).withOAuth2Credential(oAuth2Credential).build();
            } else {
                if (this.clientCustomerId == null) {
                    throw new IllegalStateException("clientCustomerId mus be set");
                }

                this.session = (new com.google.api.ads.adwords.lib.client.AdWordsSession.Builder()).withClientCustomerId(this.clientCustomerId).withDeveloperToken(this.developerToken).withUserAgent(this.userAgent).withOAuth2Credential(oAuth2Credential).build();
            }

        }
    }

    public void executeQuery() throws Exception {
        ReportingConfiguration reportingConfiguration = (new com.google.api.ads.adwords.lib.client.reporting.ReportingConfiguration.Builder()).skipReportHeader(true).skipReportSummary(!this.deliverTotalsDataset).build();
        session.setReportingConfiguration(reportingConfiguration);
        session.setValidateOnly(true);
        ReportDownloader downloader = new ReportDownloader(this.session);
        downloader.setReportDownloadTimeout(this.reportDownloadTimeout);
        if (this.sendReportAsAWQL && !this.useAWQL) {
            if (this.fields == null) {
                throw new IllegalStateException("No fields has been set!");
            }

            if (this.reportType == null) {
                throw new IllegalStateException("No reportType has been set!");
            }

            this.buildAWQLFromReportDefinition();
        }

       if (!this.sendReportAsAWQL && !this.useAWQL) {
            this.setupReportName();
            this.setupReportDefinition();
            this.response = downloader.downloadReport(this.reportDefinition);
        } else {
            if (this.awql == null) {
                throw new IllegalStateException("No AWQL has been set!");
            }

            if (!this.sendReportAsAWQL) {
                AWQLParser parser = new AWQLParser();
                parser.parse(this.awql);
                this.reportType = parser.getReportType();
                this.fields = parser.getFieldsAsString();
            }

            String awqlRequestFormat = AWQLParser.buildRequestFormat(this.awql);
            if (this.debug) {
                this.info("AWQL request formatted:" + awqlRequestFormat);
            }

            this.setupReportName();
            this.response = downloader.downloadReport(awqlRequestFormat, this.downloadFormat);
        }

        this.responseInputStream = this.response.getInputStream();
    }

    public void downloadAsFile() throws Exception {
        this.buildDownloadFile();
        this.info("Download report to: " + this.reportDownloadFilePath);
        this.httpStatus = this.response.getHttpStatus();
        this.response.saveToFile(this.reportDownloadFilePath);
        this.info("Finished.");
    }

    private File buildDownloadFile() {
        if (this.reportDownloadFilePath != null) {
            File df = new File(this.reportDownloadFilePath);
            if (!df.getParentFile().exists()) {
                df.getParentFile().mkdirs();
            }

            return df;
        } else if (this.downloadDir == null) {
            throw new IllegalStateException("Download dir not set!");
        } else if (this.reportType == null) {
            throw new IllegalStateException("Report-Type not set!");
        } else if (this.reportName == null) {
            throw new IllegalStateException("Report-Name not set!");
        } else if (this.downloadFormat == null) {
            throw new IllegalStateException("Download format not set!");
        } else {
            String fileExtension = null;
            if (this.downloadFormat == DownloadFormat.CSV) {
                fileExtension = ".csv";
            } else if (this.downloadFormat == DownloadFormat.CSVFOREXCEL) {
                fileExtension = ".bom.csv";
            } else if (this.downloadFormat == DownloadFormat.TSV) {
                fileExtension = ".tsv";
            } else if (this.downloadFormat == DownloadFormat.XML) {
                fileExtension = ".xml";
            } else if (this.downloadFormat == DownloadFormat.GZIPPED_CSV) {
                fileExtension = ".csv.gz";
            } else if (this.downloadFormat == DownloadFormat.GZIPPED_XML) {
                fileExtension = ".xml.gz";
            }

            File df = new File(this.downloadDir, this.reportName + fileExtension);
            if (!df.getParentFile().exists()) {
                df.getParentFile().mkdirs();
            }

            this.reportDownloadFilePath = df.getAbsolutePath();
            return df;
        }
    }

    private void buildAWQLFromReportDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        sb.append(this.fields);
        sb.append(" from ");
        sb.append(this.reportType);
        if (this.awqlWhereClause != null) {
            sb.append(" where ");
            sb.append(this.awqlWhereClause);
        }

        sb.append(" during ");
        sb.append(this.startDateStr);
        sb.append(",");
        sb.append(this.endDateStr);
        this.awql = sb.toString();
    }

    private ReportDefinition setupReportDefinition() {
        if (isEmpty(this.startDateStr)) {
            throw new IllegalStateException("Start date is not set");
        } else if (isEmpty(this.endDateStr)) {
            throw new IllegalStateException("End date is not set");
        } else {
            Selector selector = new Selector();
            selector.getFields().addAll(this.buildFieldList());
            DateRange dr = new DateRange();
            dr.setMin(this.startDateStr);
            dr.setMax(this.endDateStr);
            selector.setDateRange(dr);
            this.reportDefinition = new ReportDefinition();
            this.reportDefinition.setSelector(selector);
            this.reportDefinition.setReportType(this.getReportDefinitionType());
            this.reportDefinition.setReportName(this.reportName);
            this.reportDefinition.setDateRangeType(ReportDefinitionDateRangeType.fromValue("CUSTOM_DATE"));
            this.reportDefinition.setDownloadFormat(this.downloadFormat);
            return this.reportDefinition;
        }
    }

    private String setupReportName() {
        if (this.reportType == null) {
            throw new IllegalStateException("reportType cannot be null");
        } else if (this.reportName != null) {
            return this.reportName;
        } else {
            if (this.awql != null) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS");
                this.reportName = this.reportType + "#" + sdf.format(new Date());
            } else {
                this.reportName = this.reportType + "#" + this.startDateStr + "-" + this.endDateStr;
            }

            return this.reportName;
        }
    }

    private ReportDefinitionReportType getReportDefinitionType() {
        if (this.reportType == null) {
            throw new IllegalStateException("The report-type must be set!");
        } else {
            return ReportDefinitionReportType.fromValue(this.reportType);
        }
    }

    private List<String> buildFieldList() {
        if (this.fields == null) {
            throw new IllegalStateException("Fields cannot be empty. Please specify the report fields!");
        } else {
            List<String> fieldList = new ArrayList();
            StringTokenizer st = new StringTokenizer(this.fields, ",;|");

            while (st.hasMoreTokens()) {
                String field = st.nextToken().trim();
                fieldList.add(field);
            }

            return fieldList;
        }
    }

    public void setStartDate(String startDate) {
        if (!isEmpty(startDate)) {
            this.startDateStr = this.checkDate(startDate);
        }

    }

    public void setStartDate(Date startDate) {
        if (startDate != null) {
            this.startDateStr = this.sdfDate.format(startDate);
        }

    }

    public void setEndDate(String endDate) {
        if (!isEmpty(endDate)) {
            this.endDateStr = this.checkDate(endDate);
        }

    }

    public void setEndDate(Date endDate) {
        if (endDate != null) {
            this.endDateStr = this.sdfDate.format(endDate);
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
        return this.reportDownloadFilePath;
    }

    public void setReportDownloadFilePath(String reportDownloadFilePath) {
        if (!isEmpty(reportDownloadFilePath)) {
            this.reportDownloadFilePath = reportDownloadFilePath;
        }

    }

    public void setDeveloperToken(String developerToken) {
        if (!isEmpty(developerToken)) {
            this.developerToken = developerToken;
        }

    }

    public void setUserAgent(String userAgent) {
        if (!isEmpty(userAgent)) {
            this.userAgent = userAgent;
        }

    }

    public void setClientCustomerId(String clientCustomerId) {
        if (!isEmpty(clientCustomerId)) {
            this.clientCustomerId = clientCustomerId;
        }

    }

    public void setClientId(String clientId) {
        if (!isEmpty(clientId)) {
            this.clientId = clientId;
        }

    }

    public void setClientSecretFile(String clientSecretFile) {
        if (!isEmpty(clientSecretFile)) {
            this.clientSecretFile = clientSecretFile;
        }

    }

    public void setKeyFile(String keyFileStr) {
        if (!isEmpty(keyFileStr)) {
            File f = new File(keyFileStr);
            if (!f.canRead()) {
                throw new IllegalArgumentException("Key file:" + keyFileStr + " cannot be read!");
            }

            this.keyFile = f;
        }

    }

    public void setServiceAccountIdEmail(String accountEmail) {
        if (!isEmpty(accountEmail)) {
            this.serviceAccountIdEmail = accountEmail;
        }

    }

    public void setUserEmail(String adWordsAccountEmail) {
        if (!isEmpty(adWordsAccountEmail)) {
            this.userEmail = adWordsAccountEmail;
        }

    }

    public void setUseServiceAccount(boolean useServiceAccount) {
        this.useServiceAccount = useServiceAccount;
    }

    public void setAdwordsPropertyFilePath(String adwordsPropertyFilePath) {
        if (!isEmpty(adwordsPropertyFilePath)) {
            this.adwordsPropertyFilePath = adwordsPropertyFilePath;
        }

    }

    public void setUsePropertyFile(boolean usePropertyFile) {
        this.usePropertyFile = usePropertyFile;
    }

    public void setAwql(String awql) {
        if (!isEmpty(awql)) {
            this.awql = awql.trim();
        }

    }

    public void setDownloadDir(String downloadDir) {
        if (!isEmpty(downloadDir)) {
            this.downloadDir = downloadDir;
        }

    }

    public void setUseClientId(boolean useClientId) {
        this.useClientId = useClientId;
    }

    public void setRefreshToken(String refreshToken) {
        if (!isEmpty(refreshToken)) {
            this.refreshToken = refreshToken;
        }

    }

    public void setClientSecret(String clientSecret) {
        if (!isEmpty(clientSecret)) {
            this.clientSecret = clientSecret;
        }

    }

    public String getReportName() {
        return this.reportName;
    }

    public void setReportName(String reportName) {
        if (!isEmpty(reportName)) {
            this.reportName = reportName;
        }

    }

    public void deliverTotalsDataset(boolean deliverTotalsDataset) {
        this.deliverTotalsDataset = deliverTotalsDataset;
    }

    public void setTimeMillisOffsetToPast(Long timeMillisOffsetToPast) {
        if (timeMillisOffsetToPast != null) {
            this.timeMillisOffsetToPast = timeMillisOffsetToPast;
        }

    }

    public void setTimeMillisOffsetToPast(Integer timeMillisOffsetToPast) {
        if (timeMillisOffsetToPast != null) {
            this.timeMillisOffsetToPast = timeMillisOffsetToPast.longValue();
        }

    }

    public String getReportType() {
        return this.reportType;
    }

    public void setReportType(String reportType) {
        if (!isEmpty(reportType)) {
            this.reportType = reportType;
        }

    }

    public boolean isDebug() {
        return this.debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public void setDownloadFormat(String format) {
        if ("CSV".equalsIgnoreCase(format)) {
            this.downloadFormat = DownloadFormat.CSV;
        } else if ("XML".equalsIgnoreCase(format)) {
            this.downloadFormat = DownloadFormat.XML;
        } else if ("TSV".equalsIgnoreCase(format)) {
            this.downloadFormat = DownloadFormat.TSV;
        } else if ("GZIPPED_CSV".equalsIgnoreCase(format)) {
            this.downloadFormat = DownloadFormat.GZIPPED_CSV;
        } else if ("GZIPPED_XML".equalsIgnoreCase(format)) {
            this.downloadFormat = DownloadFormat.GZIPPED_XML;
        } else {
            if (!"CSVFOREXCEL".equalsIgnoreCase(format)) {
                throw new IllegalArgumentException("Download format:" + format + " is not supported!");
            }

            this.downloadFormat = DownloadFormat.CSVFOREXCEL;
        }

    }

    public boolean downloadIsAnArchive() {
        return this.downloadFormat == DownloadFormat.GZIPPED_CSV || this.downloadFormat == DownloadFormat.GZIPPED_XML;
    }

    public int getHttpStatus() {
        return this.httpStatus;
    }

    public void info(String message) {
        if (this.logger != null) {
            this.logger.info(message);
        } else {
            System.out.println("INFO:" + message);
        }

    }

    public void debug(String message) {
        if (this.logger != null) {
            this.logger.debug(message);
        } else {
            System.out.println("DEBUG:" + message);
        }

    }

    public void warn(String message) {
        if (this.logger != null) {
            this.logger.warn(message);
        } else {
            System.err.println("WARN:" + message);
        }

    }

    public void error(String message, Exception e) {
        if (this.logger != null) {
            this.logger.error(message, e);
        } else {
            System.err.println("ERROR:" + message);
        }

    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public void sendReportAsAWQL(boolean createAWQL) {
        this.sendReportAsAWQL = createAWQL;
    }

    public void setAWQLWhereClause(String awqlWhereClause) {
        if (!isEmpty(awqlWhereClause)) {
            this.awqlWhereClause = awqlWhereClause;
        }

    }

    public boolean isUseAWQL() {
        return this.useAWQL;
    }

    public void setUseAWQL(boolean useAWQL) {
        this.useAWQL = useAWQL;
    }

    public InputStream getResponseInputStream() {
        return this.responseInputStream;
    }

    public String getFields() {
        return this.fields;
    }

    public void setFields(String fields) {
        if (!isEmpty(fields)) {
            this.fields = fields;
        }

    }
}
