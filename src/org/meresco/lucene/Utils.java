/* begin license *
 *
 * "Meresco Lucene" is a set of components and tools to integrate Lucene (based on PyLucene) into Meresco
 *
 * Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
 * Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
 * Copyright (C) 2016 Stichting Kennisnet http://www.kennisnet.nl
 *
 * This file is part of "Meresco Lucene"
 *
 * "Meresco Lucene" is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * "Meresco Lucene" is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with "Meresco Lucene"; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * end license */

package org.meresco.lucene;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Formatter;
import java.util.List;

import org.apache.lucene.util.FixedBitSet;

public class Utils {
	public static String getStackTrace(Throwable aThrowable) {
		/*
		 * shameless partial copy from:
		 * http://www.javapractices.com/topic/TopicAction.do?Id=78
		 */
		final Writer result = new StringWriter();
		final PrintWriter printWriter = new PrintWriter(result);
		aThrowable.printStackTrace(printWriter);
		return result.toString();
	}

	public static void writeToStream(String response, OutputStream stream) {
		try {
			Writer writer = new BufferedWriter(new OutputStreamWriter(stream, "UTF-8"));
			writer.write(response, 0, response.length());
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String readFully(Reader reader) throws IOException {
		char[] arr = new char[8 * 1024];
		StringBuilder buf = new StringBuilder();
		int numChars;
		while ((numChars = reader.read(arr, 0, arr.length)) > 0) {
			buf.append(arr, 0, numChars);
		}
		return buf.toString();
	}

	public static String readFully(InputStream inputStream) throws IOException {
		return readFully(new InputStreamReader(inputStream));
	}

	static public String join(List<?> list, String conjunction) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Object item : list) {
			if (first)
				first = false;
			else
				sb.append(conjunction);
			sb.append(item.toString());
		}
		return sb.toString();
	}

	public static String byteToHex(final byte[] hash) {
		Formatter formatter = new Formatter();
		for (byte b : hash) {
			formatter.format("%02x", b);
		}
		String result = formatter.toString();
		formatter.close();
		return result;
	}

	public static FixedBitSet readFixedBitSet(InputStream input) throws IOException {
	    DataInputStream dis = new DataInputStream(input);
        int numBits = dis.readInt();
        int numWords = dis.readInt();
        long[] bits = new long[numWords];
        for (int i = 0; i < bits.length; i++) {
            bits[i] = dis.readLong();
        }
        return new FixedBitSet(bits, numBits);
	}

    public static void writeFixedBitSet(FixedBitSet bitSet, OutputStream output) throws IOException {
        DataOutputStream dos = new DataOutputStream(output);
        dos.writeInt(bitSet.length());
        long[] bits = bitSet.getBits();
        dos.writeInt(bits.length);
        for (int i = 0; i < bits.length; i++) {
            dos.writeLong(bits[i]);
        }
        dos.flush();
    }
}
