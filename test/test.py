import functions
import json


if __name__ == "__main__":
    #scm_doc = functions.get_station_scm(None, 'RMPW12')
    #print functions.readings_vars_additions_from_db(None, functions.reading_vars_from_scm(scm_doc))
    #print json.dumps(functions.get_stations_properties('RMPW18'))
    #print functions.make_station_property_lookup()
    print functions.get_property_col_names_station(None, 'RMPW12')