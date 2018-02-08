package com.adwords.google.desj;


import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Util {
    private static final Map<String, NumberFormat> numberformatMap = new HashMap();

    public Util() {
    }

    public static NumberFormat getNumberFormat(String localeStr) {
        if (localeStr == null) {
            localeStr = "en";
        }

        String key = localeStr + "-" + Thread.currentThread().getName();
        NumberFormat nf = (NumberFormat) numberformatMap.get(key);
        if (nf == null) {
            Locale locale = new Locale(localeStr);
            nf = NumberFormat.getInstance(locale);
            numberformatMap.put(key, nf);
        }

        return nf;
    }

    public static List<Object> convertToList(String valuesSeparated, String dataType, String delimiter, String pattern) throws Exception {
        List<Object> resultList = new ArrayList();
        StringTokenizer st = new StringTokenizer(valuesSeparated, delimiter);

        while (st.hasMoreElements()) {
            String value = st.nextToken();
            resultList.add(convertToDatatype(value, dataType, pattern));
        }

        return resultList;
    }

    public static Object convertToDatatype(String value, String dataType, String options) throws Exception {
        if (value != null && !value.trim().isEmpty()) {
            if ("String".equalsIgnoreCase(dataType)) {
                return value;
            } else if ("BigDecimal".equalsIgnoreCase(dataType)) {
                return convertToBigDecimal(value);
            } else if ("Boolean".equalsIgnoreCase(dataType)) {
                return convertToBoolean(value);
            } else if ("Date".equalsIgnoreCase(dataType)) {
                return convertToDate(value, options);
            } else if ("Double".equalsIgnoreCase(dataType)) {
                return convertToDouble(value, options);
            } else if ("Float".equalsIgnoreCase(dataType)) {
                return convertToFloat(value, options);
            } else if ("Short".equalsIgnoreCase(dataType)) {
                return convertToShort(value, options);
            } else if (!"Int".equalsIgnoreCase(dataType) && !"Integer".equalsIgnoreCase(dataType)) {
                if ("Long".equalsIgnoreCase(dataType)) {
                    return convertToLong(value, options);
                } else if ("Timestamp".equalsIgnoreCase(dataType)) {
                    return convertToTimestamp(value, options);
                } else {
                    throw new Exception("Unsupported dataType:" + dataType);
                }
            } else {
                return convertToInteger(value, options);
            }
        } else {
            return null;
        }
    }

    public static Date convertToDate(String dateString, String pattern) throws Exception {
        if (dateString != null && !dateString.trim().isEmpty()) {
            if (pattern != null && !pattern.isEmpty()) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat(pattern);
                    return sdf.parse(dateString.trim());
                } catch (Throwable var3) {
                    throw new Exception("Failed to convert string to date:" + var3.getMessage(), var3);
                }
            } else {
                throw new Exception("convertToDate failed: pattern cannot be null or empty");
            }
        } else {
            return null;
        }
    }

    public static Timestamp convertToTimestamp(String dateString, String pattern) throws Exception {
        Date date = convertToDate(dateString, pattern);
        return date != null ? new Timestamp(date.getTime()) : null;
    }

    public static Boolean convertToBoolean(String value) throws Exception {
        if (value != null && !value.trim().isEmpty()) {
            if ("true".equals(value.trim())) {
                return true;
            } else if ("false".equals(value.trim())) {
                return false;
            } else {
                throw new Exception("Value:" + value + " is not a boolean value!");
            }
        } else {
            return null;
        }
    }

    public static Double convertToDouble(String value, String locale) throws Exception {
        return value != null && !value.trim().isEmpty() ? getNumberFormat(locale).parse(value.trim()).doubleValue() : null;
    }

    public static Integer convertToInteger(String value, String locale) throws Exception {
        return value != null && !value.trim().isEmpty() ? getNumberFormat(locale).parse(value.trim()).intValue() : null;
    }

    public static Short convertToShort(String value, String locale) throws Exception {
        return value != null && !value.trim().isEmpty() ? getNumberFormat(locale).parse(value.trim()).shortValue() : null;
    }

    public static Float convertToFloat(String value, String locale) throws Exception {
        return value != null && !value.trim().isEmpty() ? getNumberFormat(locale).parse(value.trim()).floatValue() : null;
    }

    public static Long convertToLong(String value, String locale) throws Exception {
        return value != null && !value.trim().isEmpty() ? getNumberFormat(locale).parse(value.trim()).longValue() : null;
    }

    public static BigDecimal convertToBigDecimal(String value) throws Exception {
        if (value != null && !value.trim().isEmpty()) {
            try {
                return new BigDecimal(value.trim());
            } catch (RuntimeException var2) {
                throw new Exception("convertToBigDecimal:" + value + " failed:" + var2.getMessage(), var2);
            }
        } else {
            return null;
        }
    }
}
