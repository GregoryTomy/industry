import duckdb as dd

connection = dd.connect('md:industry')

connection.sql("SHOW DATABASES").show()


query = """
    select * from main_mart.industry_projection
"""

try:
    df = connection.execute(query).fetch_df()
    print("Data fetched successfully!")

    output_file = "output_data.csv"

    df.to_csv(output_file, index=False)

    print(f"Data saved to {output_file}")
except Exception as e:
    print(f"An error occured: {e}")
finally:
    connection.close()
    print("Connection closed.")
