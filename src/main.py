from DataManager import DataManager
import sys

if '--skip-validation' in sys.argv[1:]:
    skip_validation = True
else:
    skip_validation = False

if '--refresh-time' in sys.argv[1:]:
    refresh_time = int(sys.argv[sys.argv.index('--refresh-time') + 1])
else:
    refresh_time = 180

simulation = False

if "--simulation" in sys.argv[1:]:
    simulation = True


dm = DataManager(skip_validation=skip_validation, interval=refresh_time, simulation=simulation)
if simulation:
    dm.start()
else:
    dm.refresh()