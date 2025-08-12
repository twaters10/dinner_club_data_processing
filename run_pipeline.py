from Steps.ingest_data import *
from Steps.data_prep import *
from Steps.load_to_s3 import *
import logging
import os
import datetime

if __name__ == "__main__":
    df = ingest_data(google_sheets_json_path = '/Users/tawate/Documents/dinner_club_data_processing/clean-terminal-468616-k8-06169f567f7e.json', 
                     spreadsheet_title = "Dinner Club Ranking (Responses)")
    
    # Apply Weights to each category
    restaurant_rankings = {
        'Food Quality (0 - 10)': {
            'weight': 0.35
        },
        'Food Portion Size (0 - 10)': {
            'weight': 0.12
        },
        'Drinks (0 - 10)': {
            'weight': 0.15
        },
        'Service (0 - 10)': {
            'weight': 0.15
        },
        'Ambience (0 - 10)': {
            'weight': 0.18
        },
        'Bathroom Quality (0 - 10)': {
            'weight': 0.05
        }
    } 

    # Convert relevant columns to numeric, handling potential errors
    cols_to_convert = ['Food Quality (0 - 10)',	
                       'Ambience (0 - 10)',
                       'Bathroom Quality (0 - 10)',
                       'Food Portion Size (0 - 10)',
                       'Service (0 - 10)',
                       'Drinks (0 - 10)']
    
    # Prep data
    df_prep = prepare_data(df, cols_to_convert)
    
    # Function to calculate weighted ranking
    def calculate_weighted_ranking(row):
        """ Calculates the weighted ranking for a given row based on the restaurant rankings.
        Args:
            row (pd.Series): A row from the DataFrame containing restaurant ratings.
            restaurant_rankings (dict): A dictionary containing the weights for each category.
        Returns:
            float: The weighted sum of the ratings for the restaurant.
        """
        weighted_sum = 0
        for category, details in restaurant_rankings.items():
            if category in row.index:  # Check if the category exists in the row
                weighted_sum += row[category] * details['weight']
        return weighted_sum   
    
    # Apply the function to each row to create a 'Weighted Ranking' column
    df_prep['Weighted Ranking'] = df_prep.apply(calculate_weighted_ranking, axis=1)

    # Load processed data to S3
    load_processed_data_to_s3(df_prep, bucket_name = 'dinner-club-tsw', csvfilename = 'dinner_club_rankings.csv')
    
    # Log the successful completion of the pipeline and run time to pipeline.log create the log file if its not created in the current directory
    # Configure logging
    logging.basicConfig(filename='pipeline.log', level=logging.INFO)
    
    # Get current time
    completion_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create log file if it doesn't exist
    if not os.path.exists('pipeline.log'):
        with open('pipeline.log', 'w') as f:
            f.write('')
        logging.info('Log file created')
    
    # Log the completion time and success message to the log file
    with open('pipeline.log', 'a') as f:
        f.write(f'Pipeline completed successfully at {completion_time}\n')
    logging.info(f"Pipeline completed successfully at {completion_time}")
    print(f"Pipeline completed successfully at {completion_time}")
