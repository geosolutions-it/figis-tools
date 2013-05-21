/*
 *  Copyright (C) 2007 - 2013 GeoSolutions S.A.S.
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
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.validation.InvalidArgumentException;
import org.apache.commons.cli2.validation.Validator;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.DataStoreFinder;
import org.geotools.jdbc.JDBCDataStoreFactory;

import static it.geosolutions.utils.db.Gml2Orcl.aquireFactory;


/**
 * @author Fabiani, Ivano Picco
 * @author ETj
 *
 */
public class Gml2Orcl extends BaseArgumentsManager
{
    private static final TrivialLogger LOGGER = new TrivialLogger(Gml2Orcl.class);

    private static final int DEFAULT_ORACLE_PORT = 1521;

    private static final String VERSION = "0.4";
    private static final String NAME = "gml2Orcl";

    private static String hostname;
    private static Integer port;
    private static String database;
    private static String schema;
    private static String user;
    private static String password;
    private static String gmlfile;
    private static String forcedInputCrs;

    private static Map<String, Serializable> orclMap = new HashMap<String, Serializable>();

    /**
     * @param args
     */
    public static void main(String[] args)
    {
        Gml2Orcl gml2orcl = new Gml2Orcl();
        if (!gml2orcl.parseArgs(args))
        {
            System.exit(1);
        }
        try
        {
            initOrclMap();
            gml2orcl.importGmlIntoOracle(new File(gmlfile));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    // ////////////////////////////////////////////////////////////////////////
    //
    // ////////////////////////////////////////////////////////////////////////



    private static void initOrclMap()
    {
        orclMap.put(JDBCDataStoreFactory.DBTYPE.key, "Oracle");
        orclMap.put(JDBCDataStoreFactory.HOST.key, hostname);
        orclMap.put(JDBCDataStoreFactory.PORT.key, port);
        orclMap.put(JDBCDataStoreFactory.DATABASE.key, database);
        orclMap.put(JDBCDataStoreFactory.SCHEMA.key, schema);
        orclMap.put(JDBCDataStoreFactory.USER.key, user);
        orclMap.put(JDBCDataStoreFactory.PASSWD.key, password);
        orclMap.put(JDBCDataStoreFactory.MINCONN.key, 1);
        orclMap.put(JDBCDataStoreFactory.MAXCONN.key, 10);
        // orclMap.put(JDBCDataStoreFactory.NAMESPACE.key,
        // "http://www.fao.org/fi");
    }

    /**
     * When loading from DTO use the params to locate factory.
     *
     * <p>
     * bleck
     * </p>
     *
     * @param params
     *
     * @return
     */
    public static DataStoreFactorySpi aquireFactory(Map params)
    {
        for (Iterator i = DataStoreFinder.getAvailableDataStores(); i.hasNext();)
        {
            DataStoreFactorySpi factory = (DataStoreFactorySpi) i.next();

            if (factory.canProcess(params))
            {
                return factory;
            }
        }

        return null;
    }

    private Option hostnameOpt;
    private Option portOpt;
    private Option databaseOpt;
    private Option schemaOpt;
    private Option userOpt;
    private Option passwordOpt;
    private Option gmlfileOpt;
    private Option inputCrsOpt;


    /**
     * Default constructor
     */
    public Gml2Orcl()
    {
        super(NAME, VERSION);
    }

    @Override
    protected void buildOptions() {

        // /////////////////////////////////////////////////////////////////////
        // Options for the command line
        // /////////////////////////////////////////////////////////////////////
        gmlfileOpt =
            optionBuilder
                .withShortName("s")
                .withLongName("gmlfile")
                .withArgument(argumentBuilder.withName("filename").withMinimum(1).withMaximum(1).create())
                .withDescription("gmlfile to import")
                .withRequired(true)
                .create();
        inputCrsOpt =
            optionBuilder
                .withShortName("c")
                .withLongName("crs")
                .withArgument(argumentBuilder.withName("crscode").withMinimum(1).withMaximum(1).create())
                .withDescription("crscode, e.g. EPSG:4326")
                .withRequired(false)
                .create();

        hostnameOpt =
            optionBuilder
                .withShortName("H")
                .withLongName("hostname")
                .withArgument(argumentBuilder.withName("hostname").withMinimum(1).withMaximum(1).create())
                .withDescription("database host")
                .withRequired(true)
                .create();
        databaseOpt =
            optionBuilder
                .withShortName("d")
                .withShortName("db")
                .withLongName("database")
                .withArgument(argumentBuilder.withName("dbname").withMinimum(1).withMaximum(1).create())
                .withDescription("database name")
                .withRequired(true)
                .create();
        schemaOpt =
            optionBuilder
                .withShortName("S")
                .withLongName("schema")
                .withArgument(argumentBuilder.withName("schema").withMinimum(1).withMaximum(1).create())
                .withDescription("database schema")
                .withRequired(true)
                .create();
        userOpt =
            optionBuilder
                .withShortName("u")
                .withLongName("user")
                .withArgument(argumentBuilder.withName("username").withMinimum(1).withMaximum(1).create())
                .withDescription("username")
                .withRequired(false)
                .create();
        passwordOpt =
            optionBuilder
                .withShortName("p")
                .withLongName("password")
                .withArgument(argumentBuilder.withName("password").withMinimum(1).withMaximum(1).create())
                .withDescription("password")
                .withRequired(false)
                .create();
        portOpt =
            optionBuilder
                .withShortName("P")
                .withLongName("port")
                .withDescription("database port")
                .withArgument(argumentBuilder.withName("portnumber").withMinimum(1).withMaximum(1)
                    .withValidator(
                    new Validator()
                    {

                        public void validate(List args) throws InvalidArgumentException
                        {
                            final int size = args.size();
                            if (size > 1)
                            {
                                throw new InvalidArgumentException(
                                    "Only one port at a time can be defined");
                            }

                            final String val = (String) args.get(0);

                            final int value = Integer.parseInt(val);
                            if ((value <= 0) || (value > 65536))
                            {
                                throw new InvalidArgumentException(
                                    "Invalid port specification");
                            }

                        }
                    }).create())
                .withRequired(false)
                .create();

        addOption(gmlfileOpt);
        addOption(inputCrsOpt);
        addOption(databaseOpt);
        addOption(hostnameOpt);
        addOption(portOpt);
        addOption(schemaOpt);
        addOption(userOpt);
        addOption(passwordOpt);

    }

    @Override
    public boolean parseArgs(String[] args)
    {
        if (!super.parseArgs(args))
        {
            return false;
        }
        gmlfile = (String) getOptionValue(gmlfileOpt);
        forcedInputCrs = (String) getOptionValue(inputCrsOpt);

        database = (String) getOptionValue(databaseOpt);
        port = hasOption(portOpt) ? Integer.valueOf((String) getOptionValue(portOpt)) : DEFAULT_ORACLE_PORT;
        schema = (String) getOptionValue(schemaOpt);
        user = hasOption(userOpt) ? (String) getOptionValue(userOpt) : null;
        password = hasOption(passwordOpt) ? (String) getOptionValue(passwordOpt) : null;
        hostname = (String) getOptionValue(hostnameOpt);

        return true;
    }

    public void importGmlIntoOracle(File gmlFile) throws Exception {

        DataStore orclDataStore = aquireFactory(orclMap).createDataStore(orclMap);
        GmlImporter.importGml(gmlFile, orclDataStore, forcedInputCrs);
    }
}
