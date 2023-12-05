
**Peer: Kaiyu Guan**

## FIFA DataFrame Despription
**PRIMARY KEY: `id`: `id` (long): Not nullable**

| #   | Feature Name                       | Description                                                         | Constraint                                                     |
| --- | ---------------------------------- | ------------------------------------------------------------------- | -------------------------------------------------------------- |
| 1   | `sofifa_id`                        | A unique identifier for each player in the FIFA database, represented as an integer. This field is nullable, which means it can have missing or null values. | Not nullable. Value should be greater than 0.                  |
| 2   | `player_url`                       | The URL of the player's profile on the sofifa website, represented as a string. This field is nullable. | Nullable.                                                      |
| 3   | `short_name`                       | The short name or nickname of the player, represented as a string. This field is nullable. | Nullable.                                                      |
| 4   | `long_name`                        | The full name of the player, represented as a string. This field is nullable. | Nullable.                                                      |
| 5   | `player_positions`                 | The primary playing position(s) of the player, represented as a string. This field is nullable. | Nullable.                                                      |
| 6   | `overall`                          | An integer representing the overall skill rating of the player. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 7   | `potential`                        | An integer representing the potential skill rating of the player. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 8   | `value_eur`                        | A double (floating-point) value representing the player's market value in euros. This field is not nullable. | Not nullable. Value should be greater than or equal to 0.      |
| 9   | `wage_eur`                         | A double (floating-point) value representing the player's weekly wage in euros. This field is not nullable. | Not nullable. Value should be greater than or equal to 0.      |
| 10  | `age`                              | An integer representing the age of the player. This field is not nullable. | Not nullable. Value should be greater than 0.                  |
| 11  | `dob`                              | A date field representing the player's date of birth. This field is not nullable. | Not nullable.                                                  |
| 12  | `height_cm`                        | An integer representing the player's height in centimeters. This field is not nullable. | Not nullable. Value should be greater than 0.                  |
| 13  | `weight_kg`                        | An integer representing the player's weight in kilograms. This field is not nullable. | Not nullable. Value should be greater than 0.                  |
| 14  | `club_team_id`                     | A double (floating-point) value representing the unique identifier of the player's club team. This field is nullable. | Nullable.                                                      |
| 15  | `club_name`                        | A string representing the name of the player's club team. This field is not nullable. | Nullable.                                                      |
| 16  | `league_name`                      | A string representing the name of the league in which the player's club team competes. This field is nullable. | Nullable.                                                      |
| 17  | `league_level`                     | An integer representing the level of the league in which the player's club team competes. This field is not nullable. | Not nullable. Value should be greater than 0.                  |
| 18  | `club_position`                    | A string representing the player's position within their club team. This field is nullable. | Nullable.                                                      |
| 19  | `club_jersey_number`               | An integer representing the player's jersey number within their club team. This field is not nullable. | Not nullable. Value should be greater than 0.                  |
| 20  | `club_loaned_from`                 | A string representing the club from which the player is on loan. This field is nullable. | Nullable.                                                      |
| 21  | `club_joined`                      | A date field representing the date the player joined their club. This field is nullable. | Nullable.                                                      |
| 22  | `club_contract_valid_until`        | An integer representing the expiration year of the player's contract with their club. This field is not nullable. | Not nullable. Value should be greater than 0.                  |
| 23  | `nationality_id`                   | An integer representing the unique identifier of the player's nationality. This field is not nullable. | Not nullable. Value should be greater than 0.                  |
| 24  | `nationality_name`                 | A string representing the player's nationality. This field is nullable. | Nullable.                                                      |
| 25  | `nation_team_id`                   | A double (floating-point) value representing the unique identifier of the player's national team. This field is nullable. | Nullable.                                                      |
| 26  | `nation_position`                  | A string representing the player's position within their national team. This field is nullable. | Nullable.                                                      |
| 27  | `nation_jersey_number`             | An integer representing the player's jersey number within their national team. This field is not nullable. | Not nullable. Value should be greater than 0.                  |
| 28  | `preferred_foot`                   | A string representing the player's preferred foot for playing. This field is nullable. | Nullable.                                                      |
| 29  | `weak_foot`                        | An integer representing the player's weak foot rating. This field is not nullable. | Not nullable. Value should be between 1 and 5.                 |
| 30  | `skill_moves`                      | An integer representing the player's skill moves rating. This field is not nullable. | Not nullable. Value should be between 1 and 5.                 |
| 31  | `international_reputation`         | An integer representing the player's international reputation. This field is not nullable. | Not nullable. Value should be between 1 and 5.                 |
| 32  | `work_rate`                        | A string representing the player's work rate. This field is nullable. | Nullable.                                                      |
| 33  | `body_type`                        | A string representing the player's body type. This field is nullable. | Nullable.                                                      |
| 34  | `real_face`                        | A string indicating whether the player's face is represented realistically in the game. This field is nullable. | Nullable.                                                      |
| 35  | `release_clause_eur`               | A string representing the release clause amount in euros. This field is nullable. | Nullable.                                                      |
| 36  | `player_tags`                      | A string representing any tags associated with the player. | Nullable.                                                      |
| 37  | `player_traits`                    | A string representing any traits associated with the player. | Nullable.                                                      |
| 38  | `pace`                             | An integer representing the player's pace rating. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 39  | `shooting`                         | An integer representing the player's shooting rating. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 40  | `passing`                          | An integer representing the player's passing rating. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 41  | `dribbling`                        | An integer representing the player's dribbling rating. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 42  | `defending`                        | An integer representing the player's defending rating. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 43  | `physic`                           | An integer representing the player's physical rating. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 44  | `attacking_crossing`               | An integer representing the player's crossing skill in attacking. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 45  | `attacking_finishing`              | An integer representing the player's finishing skill in attacking. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 46  | `attacking_heading_accuracy`       | An integer representing the player's heading accuracy in attacking. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 47  | `attacking_short_passing`          | An integer representing the player's short passing skill in attacking. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 48  | `attacking_volleys`                | An integer representing the player's volleys skill in attacking. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 49  | `skill_dribbling`                  | An integer representing the player's dribbling skill level. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 50  | `skill_curve`                      | An integer representing the player's curve skill level. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 51  | `skill_fk_accuracy`                | An integer representing the player's free-kick accuracy skill level. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 52  | `skill_long_passing`               | An integer representing the player's long passing skill level. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 53  | `skill_ball_control`               | An integer representing the player's ball control skill level. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 54  | `movement_acceleration`            | An integer representing the player's acceleration skill in movement. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 55  | `movement_sprint_speed`            | An integer representing the player's sprint speed skill in movement. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 56  | `movement_agility`                 | An integer representing the player's agility skill in movement. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 57  | `movement_reactions`               | An integer representing the player's reaction skill in movement. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 58  | `movement_balance`                 | An integer representing the player's balance skill in movement. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 59  | `power_shot_power`                 | An integer representing the player's shot power skill. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 60  | `power_jumping`                    | An integer representing the player's jumping skill. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 61  | `power_stamina`                    | An integer representing the player's stamina skill. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 62  | `power_strength`                   | An integer representing the player's strength skill. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 63  | `power_long_shots`                 | An integer representing the player's long shots skill. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 64  | `mentality_aggression`             | An integer representing the player's aggression mentality. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 65  | `mentality_interceptions`          | An integer representing the player's interceptions skill in mentality. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 66  | `mentality_positioning`            | An integer representing the player's positioning skill in mentality. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 67  | `mentality_vision`                 | An integer representing the player's vision skill in mentality. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 68  | `mentality_penalties`              | An integer representing the player's penalty-taking skill in mentality. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 69  | `mentality_composure`              | A string representing the player's composure skill in mentality. | Nullable.                                                      |
| 70  | `defending_marking_awareness`     | An integer representing the player's marking awareness skill in defending. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 71  | `defending_standing_tackle`        | An integer representing the player's standing tackle skill in defending. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 72  | `defending_sliding_tackle`         | An integer representing the player's sliding tackle skill in defending. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 73  | `goalkeeping_diving`               | An integer representing the player's diving skill in goalkeeping. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 74  | `goalkeeping_handling`             | An integer representing the player's handling skill in goalkeeping. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 75  | `goalkeeping_kicking`              | An integer representing the player's kicking skill in goalkeeping. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 76  | `goalkeeping_positioning`          | An integer representing the player's positioning skill in goalkeeping. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 77  | `goalkeeping_reflexes`             | An integer representing the player's reflexes skill in goalkeeping. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 78  | `goalkeeping_speed`                | An integer representing the player's speed skill in goalkeeping. This field is not nullable. | Not nullable. Value should be between 1 and 99.                |
| 79  | `ls`                               | A string representing the player's position as a left striker. This field is nullable. | Nullable.                                                      |
| 80  | `st`                               | A string representing the player's position as a striker. This field is nullable. | Nullable.                                                      |
| 81  | `rs`                               | A string representing the player's position as a right striker. This field is nullable. | Nullable.                                                      |
| 82  | `lw`                               | A string representing the player's position as a left-winger. This field is nullable. | Nullable.                                                      |
| 83  | `lf`                               | A string representing the player's position as a left forward. This field is nullable. | Nullable.                                                      |
| 84  | `cf`                               | A string representing the player's position as a center forward. This field is nullable. | Nullable.                                                      |
| 85  | `rf`                               | A string representing the player's position as a right forward. This field is nullable. | Nullable.                                                      |
| 86  | `rw`                               | A string representing the player's position as a right-winger. This field is nullable. | Nullable.                                                      |
| 87  | `lam`                              | A string representing the player's position as a left attacking midfielder. This field is nullable. | Nullable.                                                      |
| 88  | `cam`                              | A string representing the player's position as a center attacking midfielder. This field is nullable. | Nullable.                                                      |
| 89  | `ram`                              | A string representing the player's position as a right attacking midfielder. This field is nullable. | Nullable.                                                      |
| 90  | `lm`                               | A string representing the player's position as a left midfielder. This field is nullable. | Nullable.                                                      |
| 91  | `lcm`                              | A string representing the player's position as a left center midfielder. This field is nullable. | Nullable.                                                      |
| 92  | `cm`                               | A string representing the player's position as a center midfielder. This field is nullable. | Nullable.                                                      |
| 93  | `rcm`                              | A string representing the player's position as a right center midfielder. This field is nullable. | Nullable.                                                      |
| 94  | `rm`                               | A string representing the player's position as a right midfielder. This field is nullable. | Nullable.                                                      |
| 95  | `lwb`                              | A string representing the player's position as a left wing-back. This field is nullable. | Nullable.                                                      |
| 96  | `ldm`                              | A string representing the player's position as a left defensive midfielder. This field is nullable. | Nullable.                                                      |
| 97  | `cdm`                              | A string representing the player's position as a center defensive midfielder. This field is nullable. | Nullable.                                                      |
| 98  | `rdm`                              | A string representing the player's position as a right defensive midfielder. This field is nullable. | Nullable.                                                      |
| 99  | `rwb`                              | A string representing the player's position as a right wing-back. This field is nullable. | Nullable.                                                      |
| 100 | `lb`                               | A string representing the player's position as a left back. This field is nullable. | Nullable.                                                      |
| 101 | `lcb`                              | A string representing the player's position as a left center back. This field is nullable. | Nullable.                                                      |
| 102 | `cb`                               | A string representing the player's position as a center back. This field is nullable. | Nullable.                                                      |
| 103 | `rcb`                              | A string representing the player's position as a right center back. This field is nullable. | Nullable.                                                      |
| 104 | `rb`                               | A string representing the player's position as a right back. This field is nullable. | Nullable.                                                      |
| 105 | `gk`                               | A string representing the player's position as a goalkeeper. This field is nullable. | Nullable.                                                      |
| 106 | `player_face_url`                  | A string representing the URL of the player's face image. This field is nullable. | Nullable.                                                      |
| 107 | `club_logo_url`                    | A string representing the URL of the club's logo. This field is nullable. | Nullable.                                                      |
| 108 | `club_flag_url`                    | A string representing the URL of the club's flag. This field is nullable. | Nullable.                                                      |
| 109 | `nation_logo_url`                  | A string representing the URL of the nation's logo. This field is nullable. | Nullable.                                                      |
| 110 | `nation_flag_url`                  | A string representing the URL of the nation's flag. This field is nullable. | Nullable.                                                      |
| 111 | `Year`                             | An integer representing the year in which the player's data is collected. This field is not nullable. | Not nullable. Value should be greater than 0.                  |
| 112 | `id`                               | A long integer serving as a unique identifier for each record in the database. This field is not nullable. | Not nullable. Value should be greater than 0.                  |
