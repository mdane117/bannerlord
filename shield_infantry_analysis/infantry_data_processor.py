import pandas as pd

# Function to check for unique unit-opponent matchups and backfill data accordingly
def backfill_data(df):
    # Get unique combinations of unit and opponent
    unique_combinations = df[['unit', 'opponent']].drop_duplicates()
    
    # Create a list to store new backfilled rows
    backfilled_rows = []

    # Iterate over each unique combination
    for index, row in unique_combinations.iterrows():
        # Check if there is an existing opposite matchup (e.g., Darkhan-Legionary for Legionary-Darkhan)
        opposite_matchups = df[(df['unit'] == row['opponent']) & (df['opponent'] == row['unit'])]
        
        # If there are no opposite matchups, we need to backfill the data
        if opposite_matchups.empty:
            # Find the original matchup rows to duplicate
            original_matchups = df[(df['unit'] == row['unit']) & (df['opponent'] == row['opponent'])]
            
            # Backfill the data for each original matchup row
            for _, original_row in original_matchups.iterrows():
                # Create a new row with reversed unit and opponent and with kills and deaths swapped
                new_row = original_row.copy()
                new_row['unit'], new_row['opponent'] = original_row['opponent'], original_row['unit']
                new_row['unit_id'], new_row['opponent_id'] = original_row['opponent_id'], original_row['unit_id']
                new_row['kills'], new_row['deaths'] = original_row['deaths'], original_row['kills']

                # Handle the exception for 'Mixed Infantry'
                if original_row['unit'] == 'Mixed Infantry':
                    new_row['unit_count'], new_row['opponent_count'] = original_row['opponent_count'], 250
                else:
                    new_row['unit_count'], new_row['opponent_count'] = original_row['opponent_count'], original_row['unit_count']
                    
                # Calculate the KDR from the kills and deaths for data integrity
                new_row['kdr'] = round(new_row['kills'] / new_row['deaths'], 2) if new_row['deaths'] != 0 else 0
                # Status is 'Win' if original was 'Loss' and vice versa; if it's a Draw, it stays the same
                new_row['status'] = 'Loss' if original_row['status'] == 'Win' else 'Win' if original_row['status'] == 'Loss' else 'Draw'
                
                # Append the new backfilled row to the list
                backfilled_rows.append(new_row)

    # Append the new backfilled rows to the original dataframe
    #backfilled_df = df.append(backfilled_rows, ignore_index=True)
    backfilled_df = pd.concat([df, pd.DataFrame(backfilled_rows)], ignore_index=True)
    
    return backfilled_df

# Read the existing data from the csv file
df_original = pd.read_csv("shield_infantry_data.csv")

# Apply the backfill function
df_combined = backfill_data(df_original)


df_combined.to_csv('shield_infantry_processed_data.csv', index= False)
