/*
 *  Copyright (C) 2007 - 2012 GeoSolutions S.A.S.
 *  http://www.geo-solutions.it
 * 
 *  GPLv3 + Classpath exception
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package it.geosolutions.utils.db;

import java.io.File;
import java.io.InputStream;


import org.geotools.GML;
import org.geotools.GML.Version;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Geometry;


/**
 *
 */
public class GmlImporter 
{
    private static final TrivialLogger LOGGER = new TrivialLogger(GmlImporter.class);

    /**
     * Import features from a GML/WFS1.0 file into a Datastore.
     *
     * @param gmlFile the File to be ingested
     * @param targetDataStore
     * @param forcedCrsCode the CRS to use for the source features, may be null
     *
     * @throws Exception
     */
    public static void importGml(File gmlFile, DataStore targetDataStore, String forcedCrsCode) throws Exception
    {
        LOGGER.log("importing gmlfile " + gmlFile);

        long startwork = System.currentTimeMillis();

        CoordinateReferenceSystem forcedCRS = null;

        if(forcedCrsCode != null) {
            forcedCRS = CRS.decode(forcedCrsCode, true); // may throw
        }


        InputStream in = gmlFile.toURI().toURL().openStream();

        GML gml = new GML(Version.WFS1_0);
        SimpleFeatureCollection featureCollection = gml.decodeFeatureCollection(in);

        String typeName = featureCollection.getSchema().getTypeName();

        /** Importing GML Data to DB **/
        SimpleFeatureType ftSchema = featureCollection.getSchema();

        SimpleFeatureType targetSchema;
		// build the schema type
		try {
			SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
			tb.setName(typeName);

			for(AttributeDescriptor att : ftSchema.getAttributeDescriptors())
	        {
	        	if(
        				!att.getLocalName().equals("name") &&
        				!att.getLocalName().equals("description") &&
        				!att.getLocalName().equals("boundedBy")
        		)
	        	{
	        		tb.add(att);
	        	}
	        }

			targetSchema = tb.buildFeatureType();
		} catch (Exception e) {
			throw new RuntimeException(
					"Failed to import data into the target store", e);
		}//try:catch

        // create the schema for the new shape file
        try
        {
            // FTWrapper pgft = new FTWrapper(shpDataStore.getSchema(ftName));
            // pgft.setReplaceTypeName(tablename);
            targetDataStore.createSchema(targetSchema);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            // Most probably the schema already exists in the DB
            LOGGER.log("Error while creating schema '" + typeName + "': " + e.getMessage());
            LOGGER.log("Will try to load featuretypes bypassing the error.");

            // orclDataStore.updateSchema(typeNames[t],
            // dataStore.getSchema(typeNames[t]));
        }

        // get a feature writer
        FeatureWriter<?, SimpleFeature> fw = targetDataStore.getFeatureWriter(typeName.toUpperCase(), Transaction.AUTO_COMMIT);

        // /////////////////////////////////////////////////////////////////////
        //
        // create the features
        //
        // /////////////////////////////////////////////////////////////////////
        SimpleFeature dstFeature = null;

        final FeatureIterator<?> fr = featureCollection.features();

        final int size = featureCollection.size();

        final CoordinateReferenceSystem sourceCRS;
        final CoordinateReferenceSystem declaredCRS =
            featureCollection.getSchema().getGeometryDescriptor().getCoordinateReferenceSystem();
        final CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:4326", true);

        if ( declaredCRS != null ) {
            if(forcedCRS != null ) {
                sourceCRS = forcedCRS;
                LOGGER.log("Forcing source CRS " + sourceCRS.getName().getCode() + "; input file declares " + declaredCRS);
            } else {
                sourceCRS = declaredCRS;
                LOGGER.log("Using source CRS " + sourceCRS.getName().getCode());
            }
        } else {
            if(forcedCRS != null ) {
                sourceCRS = forcedCRS;
                LOGGER.log("Using forced CRS: " + sourceCRS.getName().getCode());
            } else {
                sourceCRS = CRS.decode("EPSG:4326", true);
                LOGGER.log("No source CRS defined; using " + sourceCRS.getName().getCode());
            }
        }

        final MathTransform srcCRSToWGS84 = CRS.findMathTransform(sourceCRS, targetCRS, true);

        if (!srcCRSToWGS84.isIdentity()) {
            LOGGER.log("Geometries will be reprojected from " + sourceCRS.getCoordinateSystem().getName().getCode()
                    + " to" + targetCRS.getCoordinateSystem().getName().getCode());
        }

        int cnt = 0;
        String currentId = null;
        Geometry trgGeom = null;
        Geometry origGeom = null;

        while (fr.hasNext())
        {
            SimpleFeature srcFeature = (SimpleFeature) fr.next();
            trgGeom = null;
            origGeom = null;
            currentId = null;

            // avoid illegal state
            fw.hasNext();
            dstFeature = fw.next();

            System.out.print("Feature #"+cnt+"\r");

            if ((cnt % 50) == 0)
            {
                LOGGER.log("inserted ft #" + cnt + "/" + size + " in " + typeName);
            }

            if (srcFeature != null)
            {
                try {
                    for (AttributeDescriptor attrDescr : srcFeature.getFeatureType().getAttributeDescriptors()) {
                        if (attrDescr != null) {
                            if (!attrDescr.getLocalName().equals("name")
                                    && !attrDescr.getLocalName().equals("description")
                                    && !attrDescr.getLocalName().equals("boundedBy")) {

                                Object attrib = srcFeature.getAttribute(attrDescr.getName());
                                if ( attrib instanceof Geometry) {

                                    /**
                                     * get the original geometry and put it as is into the DB ... *
                                     */
                                    Geometry defGeom = (Geometry) attrib;
                                    origGeom = defGeom;

                                    /**
                                     * if we need to reproject the geometry before inserting into the DB ... *
                                     */
                                    if (!srcCRSToWGS84.isIdentity()) {
                                        defGeom = JTS.transform((Geometry) attrDescr, srcCRSToWGS84);
                                    }

                                    defGeom.setSRID(4326);
                                    trgGeom = defGeom.buffer(0);

                                    if(trgGeom.isEmpty()) {
                                        LOGGER.log("ALERT: the zero-buffer geom is empty. Using the original one");
                                        LOGGER.log("Src geometry:" + defGeom);
                                        LOGGER.log("Trg geometry:" + trgGeom);

                                        trgGeom = defGeom;
                                    }

                                    dstFeature.setAttribute(attrDescr.getName(), trgGeom);
                                } else {
                                    dstFeature.setAttribute(attrDescr.getName(), attrib);
                                }
                            }
                        }
                    }
                    currentId = dstFeature.getID();

                    fw.write();
                } catch (Exception ex) {
                    System.out.println("");
                    LOGGER.log("Exception on feature #"+cnt + ", ID:" +currentId +" : " + ex.getMessage());
                    LOGGER.log("Src geometry:" + origGeom);
                    LOGGER.log("Trg geometry:" + trgGeom);
                    throw ex;
                }
                cnt++;
            }
        }
        fr.close();

        try
        {
            fw.close();
        }
        catch (Exception whatever)
        {
            // amen
        }

        /** Importing SHP Data to DB - END **/
        long endwork = System.currentTimeMillis();

        LOGGER.log(" *** Inserted " + cnt + " features in " + typeName + " in " + (endwork - startwork) + "ms");
    }
}
