package com.projects.rabey;

import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] a, byte[] b) {
        if (a == b)
            return 0;
        if (a == null)
            return -1;
        if (b == null)
            return 1;

        int minLength = Math.min(a.length, b.length);
        for (int i = 0; i < minLength; i++) {
            int diff = Integer.compare(a[i] & 0xFF, b[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return Integer.compare(a.length, b.length);
    }
}