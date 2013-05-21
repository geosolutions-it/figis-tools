/*
 *  Copyright (C) 2013 GeoSolutions S.A.S.
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
/**
 *
 */
package it.geosolutions.utils.db;

import java.io.File;
import java.util.Map;


import org.apache.commons.cli2.Option;
import org.geotools.data.DataStore;

import java.util.Collections;
import org.geotools.data.FileDataStoreFactorySpi;
import org.geotools.data.FileDataStoreFinder;


/**
 * @author ETj
 *
 */
public class Gml2shp extends BaseArgumentsManager
{
    private static final TrivialLogger LOGGER = new TrivialLogger(Gml2shp.class);


    private static final String VERSION = "0.3";
    private static final String NAME = "gml2shp";

    private static String gmlfile;
    private static String forcedInputCrs;
    private static String shpfile;

    private Option inputFileOpt;
    private Option inputCrsOpt;
    private Option outputFileOpt;


    /**
     * @param args
     */
    public static void main(String[] args)
    {
        Gml2shp g2s = new Gml2shp();
        if (!g2s.parseArgs(args))
        {
            System.exit(1);
        }
        try
        {
            g2s.importGmlIntoShp(new File(gmlfile), new File(shpfile));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Default constructor
     */
    public Gml2shp()
    {
        super(NAME, VERSION);
    }

    @Override
    protected void buildOptions() {
        // /////////////////////////////////////////////////////////////////////
        // Options for the command line
        // /////////////////////////////////////////////////////////////////////
        inputFileOpt = optionBuilder
                .withShortName("i")
                .withLongName("gmlfile")
                .withArgument(
                    argumentBuilder
                        .withName("gmlfilename")
                        .withMinimum(1).withMaximum(1).create())
                .withDescription("input GML file")
                .withRequired(true).create();

        inputCrsOpt =
            optionBuilder
                .withShortName("c")
                .withLongName("crs")
                .withArgument(argumentBuilder.withName("crscode").withMinimum(1).withMaximum(1).create())
                .withDescription("crscode, e.g. EPSG:4326")
                .withRequired(false)
                .create();


        outputFileOpt = optionBuilder
                .withShortName("o")
                .withLongName("outfile")
                .withArgument(
                    argumentBuilder
                        .withName("shpfilename")
                        .withMinimum(1).withMaximum(1).create())
                .withDescription("output SHP file")
                .withRequired(true).create();

        addOption(inputFileOpt);
        addOption(inputCrsOpt);
        addOption(outputFileOpt);
    }

    @Override
    public boolean parseArgs(String[] args)
    {
        if (!super.parseArgs(args))
        {
            return false;
        }

        gmlfile = (String) getOptionValue(inputFileOpt);
        forcedInputCrs = (String) getOptionValue(inputCrsOpt);

        shpfile = (String) getOptionValue(outputFileOpt);

        return true;
    }


    public void importGmlIntoShp(File gmlFile, File shpFile) throws Exception {

        FileDataStoreFactorySpi factory = FileDataStoreFinder.getDataStoreFactory("shp");
        Map map = Collections.singletonMap( "url", shpFile.toURI().toURL() );

        DataStore datastore = factory.createNewDataStore( map );
        GmlImporter.importGml(gmlFile, datastore, forcedInputCrs);
    }

}
