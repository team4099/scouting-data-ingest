from DataManager import DataManager
import sys



try:
    if '--skip-validation' in sys.argv[1:]:
        dm = DataManager(skip_validation=True)
    else:
        dm = DataManager(skip_validation=False)
    dm.get_data()
    dm.check_data()
    dm.calculate_data()
except Exception as e:
    print(e)
