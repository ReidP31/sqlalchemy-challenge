import numpy as np

from datetime import datetime

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# grab the table names from the db: ['measurement', 'station']
Base.classes.keys()

# Save references to tables
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/YYYY-MM-DD<br/>"
        f"/api/v1.0/YYYY-MM-DD/YYYY-MM-DD"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    """Return a list of Dates and their Precipitation Values"""
    # Query to retrieve the last 12 months of precipitation data
    prcp_results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date > '2016-08-22').order_by(Measurement.date).all()
 
    session.close()

    prcp_dates = list(prcp_results)

    return jsonify(prcp_dates)

    

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all station names"""
    # Query all stations
    station_results = session.query(Measurement.station).group_by(Measurement.station).order_by(func.count(Measurement.station).desc())

    session.close()

    all_stations = list(station_results)

    return jsonify(all_stations)


@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of Dates and their Temperature Values"""
    # Query all stations
    tobs_results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date > '2016-08-22').filter(Measurement.station == 'USC00519281').all()

    session.close()

    # Create a dictionary from the row data and append to a list of the tobs values for the last 12 months
    last_12_mo_tobs = []
    for date, temp in tobs_results:
        tobs_dict = {}
        tobs_dict["Date"] = date
        tobs_dict["Temperature"] = temp
        last_12_mo_tobs.append(tobs_dict)

    return jsonify(last_12_mo_tobs)


@app.route("/api/v1.0/<start_date>")
def start_date(start_date):
    # Create our session (link) from Python to the DB
    session = Session(engine)


    """Fetch the minimum, average, and maximum temperature for all dates greater than and equal to the given start date."""

    # Query to find min, avg, max temps for all dates greater than or equal to a given start date
    start_date_results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start_date).all()

    session.close()

    # Convert Given Start Date and Final Date of Dataset to datetime objects for comparison later
    start_dt = datetime.strptime(start_date,'%Y-%m-%d')
    final_date_dt = datetime.strptime('2017-08-23','%Y-%m-%d')
    

    # Create a dictionary from the row data and append to a list
    min_avg_max_temps = []
    for _min, _avg, _max in start_date_results:
        sd_dict = {}
        sd_dict["Min. Temperature"] = _min
        sd_dict["Avg. Temperature"] = _avg
        sd_dict["Max. Temperature"] = _max
        min_avg_max_temps.append(sd_dict)
        

        if start_dt <= final_date_dt:     
            return jsonify(min_avg_max_temps)

    return jsonify({"error": f"Start Date '{start_date} not found in database."}), 404


@app.route("/api/v1.0/<start_date>/<end_date>")
def start_end(start_date, end_date):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Fetch the minimum, average, and maximum temperature for all dates included in the range between the given start and end dates."""
    first_date = session.query(Measurement.date).order_by(Measurement.date).first()
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    print("First Date", first_date)
    print("Last Date:", last_date)

    # Query to find min, avg, max temps for all dates greater than or equal to a given start date and less than or equal to a given end date
    start_end_results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

    session.close()

    # Convert dates to datetime objects for comparison later
    start_date_dt = datetime.strptime(start_date,'%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date,'%Y-%m-%d')
    first_date_dt = datetime.strptime('2010-01-01','%Y-%m-%d')
    final_date_dt = datetime.strptime('2017-08-23','%Y-%m-%d')

    # Create a dictionary from the row data and append to a list
    min_avg_max_start_end_temps = []
    for _min, _avg, _max in start_end_results:
        se_dict = {}
        se_dict["Min. Temperature"] = _min
        se_dict["Avg. Temperature"] = _avg
        se_dict["Max. Temperature"] = _max
        min_avg_max_start_end_temps.append(se_dict)
        

        if start_date_dt >= first_date_dt and end_date_dt <= final_date_dt:     
            return jsonify(min_avg_max_start_end_temps)

    return jsonify({"error": f"Start Date/End Date pair error. Please try again. Note: The minimum start date is 2010-01-01 and the maximum end date is 2017-08-23."}), 404



if __name__ == '__main__':
    app.run(debug=True)
   
