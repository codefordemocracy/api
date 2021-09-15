import datetime
import math

#########################################################
# dates
#########################################################

# set default min and max years
def get_years():
    return {
        "calendar": {
            "min": 1990,
            "max": datetime.datetime.now().year
        },
        "default": {
            "min": 2021,
            "max": 2022
        }
    }

# set default min and max cycles
def get_cycles():
    return {
        "min": 2016,
        "max": math.ceil(datetime.datetime.now().year/2.)*2,
        "current": 2022
    }
