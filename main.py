import logging
import time

from sensedata_api import SensedataAPI
from stitch_api import StitchApi

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    # Starts Sensedata API services
    sense_data_api = SensedataAPI()

    # Starts Stitch API services
    stitch = StitchApi()

    # List with all Sensedata entities that
    # must sync with sticth
    entities = ['contacts', 'customers', 'nps', 'tasks']

    for entity in entities:

        for page_number in range(1, 500):
            temp_data = sense_data_api.get_entity_data(entity_name=entity, page=page_number)

            # Count is 0 when pagination ends
            if temp_data['count'] == 0:
                break

            # logs the page end result len
            logger.info(f'page: {page_number}, rows= {len(temp_data[entity])}')

            # makes json result as stitch object
            object_as_str = stitch.paser_entity_data_to_stitch_standard(data=temp_data[entity],
                                                                        entity_name=entity)

            # Pushes data to stitch server
            stitch.push_data_to_stitch(data=object_as_str)
            time.sleep(3)

    logger.info("sync has been finished")
