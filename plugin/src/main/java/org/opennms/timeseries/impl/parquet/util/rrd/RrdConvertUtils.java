/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2013-2014 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2014 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/
package org.opennms.timeseries.impl.parquet.util.rrd;

import java.io.*;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.sax.SAXSource;

import org.jrobin.core.RrdDb;
import org.jrobin.core.RrdException;
import org.opennms.timeseries.impl.parquet.util.rrd.v1.RRDv1;
import org.opennms.timeseries.impl.parquet.util.rrd.v3.RRDv3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * The Class RRD Conversion Utilities.
 *
 * @author <a href="mailto:agalue@opennms.org">Alejandro Galue</a>
 */
public class RrdConvertUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RrdConvertUtils.class);

    /**
     * Instantiates a new RRDtool Convert Utils.
     */
    private RrdConvertUtils() {}

    /**
     * Dumps a JRB.
     *
     * @param sourceFile the source file
     * @return the RRD Object (old version)
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws RrdException the RRD exception
     */
    public static RRDv1 dumpJrb(File sourceFile) throws IOException, RrdException, JAXBException {
        RrdDb jrb = new RrdDb(sourceFile, true);
        JAXBContext jc = JAXBContext.newInstance(RRDv1.class);
        Unmarshaller jaxbUnmarshaller = jc.createUnmarshaller();
        StringReader reader = new StringReader(jrb.getXml());
        RRDv1 rrd = (RRDv1) jaxbUnmarshaller.unmarshal(reader);
        jrb.close();
        return rrd;
    }

    /**
     * Dumps a RRD.
     *
     * @param sourceFile the source file
     * @return the RRD Object
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws RrdException the RRD exception
     */
    public static RRDv3 dumpRrd(File sourceFile) throws IOException, RrdException {
        String rrdBinary = System.getProperty("rrd.binary");
        if (rrdBinary == null) {
            throw new IllegalArgumentException("rrd.binary property must be set");
        }
        try {
            XMLReader xmlReader = XMLReaderFactory.createXMLReader();
            xmlReader.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            Process process = Runtime.getRuntime().exec(new String[] {rrdBinary, "dump", sourceFile.getAbsolutePath()});
            SAXSource source = new SAXSource(xmlReader, new InputSource(new InputStreamReader(process.getInputStream())));
            JAXBContext jc = JAXBContext.newInstance(RRDv3.class);
            Unmarshaller u = jc.createUnmarshaller();
            return (RRDv3) u.unmarshal(source);
        } catch (Exception e) {
            throw new RrdException("Can't parse RRD Dump", e);
        }
    }
}