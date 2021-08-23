import os
from unittest import TestCase, mock, main
from datetime import date
from service_journal.sql_handler import config
from service_journal.sql_handler.query_builder import build_query, QueryTypes

ENV_EDITS = {
    'JOURNAL_USE_CONFIG_FILE': 'true',
    # 'JOURNAL_FORCE_USE_CONFIG_FILE': 'false',
    'JOURNAL_CONFIG_NAME': 'test_config.json'
}


from_date, to_date = date(2020, 1, 30), date(2020, 1, 31)


with mock.patch.dict(os.environ, ENV_EDITS):
    config.setup()
    print(config.config)


def normalize_time(datetime_obj):
    try:
        return datetime_obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
    except AttributeError:
        return datetime_obj


def check_scheduled_equivalence(schedule, expected, print_inequality=True):
    for date_, blocks in expected.items():
        for block, trips in blocks.items():
            for trip, trip_data in trips.items():
                sched_trip = schedule[date_][block][trip]
                if sched_trip['seq_tracker'] != trip_data['seq_tracker'] or sched_trip['route'] != trip_data['route']:
                    if print_inequality:
                        print(f'Inequality occurred on: {date_}-{block}-{trip}')
                        print(f'Expected: {trip_data}\n\n')
                        print(f'Got: {sched_trip}\n\n')
                    return False
                # Check stop order
                expected_stops = list(trip_data['stops'].keys())
                scheduled_stops = list(sched_trip['stops'].keys())
                if expected_stops != scheduled_stops:
                    if print_inequality:
                        print(f'Inequality occurred on: {date_}-{block}-{trip}')
                        print(f'Expected stops: {expected_stops}\n\n')
                        print(f'Got stops: {scheduled_stops}\n\n')
                    return False
                for stop_num, stop_data in trip_data['stops'].items():
                    for stop_datum_name, stop_datum_value in stop_data.items():
                        if stop_datum_value != sched_trip['stops'][stop_num][stop_datum_name]:
                            if print_inequality:
                                print(f'Inequality occurred on: {date_}-{block}-{trip}-stop:{stop_num}')
                                print(f'Expected: {stop_datum_value}\n\n')
                                print(f'Got: {sched_trip["stops"][stop_num][stop_datum_name]}\n\n')
                            return False
    return True


def check_actuals_equivalence(actuals, expected, print_inequality=True):
    for date_, buses in expected.items():
        for bus, reports in buses.items():
            for time_, report in reports.items():
                for datum_name, datum_value in report.items():
                    if datum_value != actuals[date_][bus][time_][datum_name]:
                        if print_inequality:
                            print(f'Inequality occurred in actuals: {date_}-{bus}-{time_}-datum:{datum_name}')
                            print(f'Expected: {datum_value}\n\n')
                            print(f'Got: {actuals[date_][bus][time_][datum_name]}\n\n')
                        return False
    return True


class QueryBuilderTests(TestCase):

    def test_select_fields(self):
        expected_query = 'SELECT one, two, three FROM my_table'
        fields = ['one', 'two', 'three']
        table = 'my_table'
        query1 = build_query('select', fields, table)
        query2 = build_query(QueryTypes.SELECT, fields, table)
        self.assertEqual(query1, expected_query, f'Query1 ({query1}) is not what was expected. ({expected_query})')
        self.assertEqual(query2, expected_query, f'Query2 ({query2}) is not what was expected. ({expected_query})')

    def test_select_filter(self):
        expected_query = 'SELECT one, two, three FROM my_table WHERE one=1'
        fields = ['one', 'two', 'three']
        table = 'my_table'
        filters = ['one=1']
        query1 = build_query('select', fields, table, filters)
        query2 = build_query(QueryTypes.SELECT, fields, table, filters)
        self.assertEqual(query1, expected_query, f'Query1 ({query1}) is not what was expected. ({expected_query})')
        self.assertEqual(query2, expected_query, f'Query2 ({query2}) is not what was expected. ({expected_query})')

    def test_select_order_by(self):
        expected_query = 'SELECT one, two, three FROM my_table ORDER BY two, three'
        fields = ['one', 'two', 'three']
        table = 'my_table'
        order_by = ['two', 'three']
        query1 = build_query('select', fields, table, order_by=order_by)
        query2 = build_query(QueryTypes.SELECT, fields, table, order_by=order_by)
        self.assertEqual(query1, expected_query, f'Query1 ({query1}) is not what was expected. ({expected_query})')
        self.assertEqual(query2, expected_query, f'Query2 ({query2}) is not what was expected. ({expected_query})')

    def test_select_special_fields(self):
        expected_query = 'SELECT one, TWO_MANY(lol), What, for free? FROM my_table'
        fields = ['one', 'two', 'three']
        table = 'my_table'
        special_fields = {'two': 'TWO_MANY(lol)', 'three': 'What, for free?'}
        query1 = build_query('select', fields, table, special_fields=special_fields)
        query2 = build_query(QueryTypes.SELECT, fields, table, special_fields=special_fields)
        self.assertEqual(query1, expected_query, f'Query1 ({query1}) is not what was expected. ({expected_query})')
        self.assertEqual(query2, expected_query, f'Query2 ({query2}) is not what was expected. ({expected_query})')
    # 'SELECT  one, TWO_MANY(lol), What, for free? FROM my_table'
    # 'SELECT one, TWO_MANY(lol), What, for free? FROM my_table'

    def test_select_filter_order_by(self):
        expected_query = 'SELECT one, TWO_MANY(lol), What, for free? FROM my_table WHERE one=1 ORDER BY two, three'
        fields = ['one', 'two', 'three']
        table = 'my_table'
        filters = ['one=1']
        order_by = ['two', 'three']
        query1 = build_query('select', fields, table, filters, order_by)
        query2 = build_query(QueryTypes.SELECT, fields, table, filters, order_by)
        self.assertEqual(query1, expected_query, f'Query1 ({query1}) is not what was expected. ({expected_query})')
        self.assertEqual(query2, expected_query, f'Query2 ({query2}) is not what was expected. ({expected_query})')


#
# class JournalProcessTest(TestCase):
#
#     def test_one_process(self):
#         test_name = 'JournalProcessTest.test_one_process'
#         with Journal(config.config) as journal:
#             journal.install_processor_preset(MAIN_PRESET)
#             journal.process_dates_batch(from_date, to_date)
#             # TODO: Read output and determine if it matches expected output


if __name__ == '__main__':
    main()
