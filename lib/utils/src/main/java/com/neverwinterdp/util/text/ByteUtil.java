package com.neverwinterdp.util.text;

import java.text.DecimalFormat;

public class ByteUtil {
  static DecimalFormat DECIMAL_FORMATER = new DecimalFormat("#.00");

  static public String byteToHumanReadable(long number) {
    if (number == 0) return "0";
    String suffix = "Bytes";
    String value = null;

    if (number > 1024 * 1024 * 1024l) {
      suffix = "GB";
      value = Double.toString(Math.round(number / (1024 * 1024 * 1024l)));
    } else if (number > 1024 * 1024l) {
      suffix = "MB";
      value = Double.toString(Math.round(number / (1024 * 1024l)));
    } else if (number > 1024l) {
      suffix = "KB";
      value = Double.toString(Math.round(number / 1024l));
    } else {
      value = Long.toString(number);
    }
    return value + "(" + suffix + ")";
  }
}
