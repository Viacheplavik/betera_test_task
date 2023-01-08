import numpy as np
import pandas as pd

INPUT_FOLDER = 'betera_datasets'
INPUT_BETS_FILE = 'bets'
INPUT_EVENTS_FILE = 'events'

OUTPUT_FOLDER = 'output_folder'
OUTPUT_FILE = 'task_1_output'

BET_CREATION_STARTS = pd.Timestamp('2022-03-14 12:00:00.000')
BET_ACCEPTANCE_DEADLINE = pd.Timestamp('2022-03-15 12:00:00.000')


class CyberSportPlayersSpecialSelector:
    def __init__(self,
                 bets_df: pd.DataFrame,
                 events_df: pd.DataFrame,
                 bet_creation_starts: pd.Timestamp,
                 bet_acceptance_deadline: pd.Timestamp
                 ):
        self.bets_df = bets_df
        self.events_df = events_df
        self.bet_creation_starts = bet_creation_starts
        self.bet_acceptance_deadline = bet_acceptance_deadline

    def players_extractor(self) -> np.ndarray:
        events_and_bets_joined_df = pd.merge(self.bets_df,
                                             self.events_df,
                                             on='event_id')

        main_condition = (
                (events_and_bets_joined_df[
                     'settlement_time'] <= self.bet_acceptance_deadline) &
                (events_and_bets_joined_df[
                     'create_time'] >= self.bet_creation_starts) &
                (events_and_bets_joined_df['sport'] == 'E-Sports') &
                (events_and_bets_joined_df['event_stage'] == 'Prematch') &
                (events_and_bets_joined_df['amount'] >= 10) &
                (events_and_bets_joined_df['bet_type'] != 'System') &
                (~events_and_bets_joined_df['is_free_bet']) &
                (events_and_bets_joined_df['result'].isin(('Lose', 'Win')))
        )

        players_suitable_odd_records = events_and_bets_joined_df[
            (events_and_bets_joined_df['accepted_odd'] >= 1.5) &
            main_condition
            ]
        players_unsuitable_odd_records = events_and_bets_joined_df[
            (events_and_bets_joined_df['accepted_odd'] < 1.5) &
            main_condition
            ]

        except_players_with_unsuitable_odds = \
            pd.unique([player for player
                       in players_suitable_odd_records['player_id']
                       if player not in players_unsuitable_odd_records[
                           'player_id']])

        return except_players_with_unsuitable_odds

    @staticmethod
    def load_list_to_csv(result: np.array) -> None:
        result_df = pd.DataFrame({'players': result})
        result_df.to_csv('{output_folder}/{output_file}.csv'.format(
            output_folder=OUTPUT_FOLDER,
            output_file=OUTPUT_FILE
        ))


def main():
    bets_df = pd.read_csv('{input_folder}/{input_bets_file}.csv'.format(
        input_folder=INPUT_FOLDER,
        input_bets_file=INPUT_BETS_FILE),
                          converters={'bet_id': str,
                                      'player_id': str,
                                      'accept_time': pd.Timestamp,
                                      'create_time': pd.Timestamp,
                                      'settlement_time': pd.Timestamp,
                                      'is_free_bet': np.bool_()
                                      }
                          )
    events_df = pd.read_csv(
        '{input_folder}/{input_events_file}.csv'.format(
            input_folder=INPUT_FOLDER,
            input_events_file=INPUT_EVENTS_FILE
        ))

    cyber_sport_players_special_selector = CyberSportPlayersSpecialSelector(
        bets_df=bets_df,
        events_df=events_df,
        bet_creation_starts=BET_CREATION_STARTS,
        bet_acceptance_deadline=BET_ACCEPTANCE_DEADLINE
    )

    result = cyber_sport_players_special_selector.players_extractor()
    cyber_sport_players_special_selector.load_list_to_csv(result)


if __name__ == '__main__':
    main()
