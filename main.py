from flask import Flask, render_template, jsonify, request,  Response, json,stream_with_context
import os
import re
import mysql.connector
from mysql.connector import errors
from imdb import IMDb
import webview
from datetime import datetime
import pytz
import time
import shutil
import threading
import sys

app = Flask(__name__)

# MySQL database connection configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Temp_passw',
    'database': 'Temp_db',
    'port':3306
}


@app.route('/check-language/<language>', methods=['GET'])
def check_language(language):
    query = "SELECT COUNT(*) AS count FROM movies1 WHERE languages LIKE %s"
    result = execute_query(query, (f"%{language}%",))
    return jsonify({'exists': result[0]['count'] > 0})

@app.route('/create-language', methods=['POST'])
def create_language():
    data = request.json
    new_language = data['language']
    new_language=new_language.title()
    current_ist_time = get_current_ist_time()
    
    # Check if the language already exists
    check_query = "SELECT * FROM movies1 WHERE languages LIKE %s"
    res = execute_query(check_query, (f"%{new_language}%",))
    
    if not res:
        insert_temp_file_query = """
            INSERT INTO movies1 (title, languages, genres, last_modified) 
            VALUES (%s, %s, %s, %s)
        """
        execute_query(insert_temp_file_query, (
            'temp_file', new_language, 'Action,Adventure,Comedy,Crime,Drama,History,Horror,Thriller,Romance,War', current_ist_time
        ), fetch=False)
    
    return '', 204

@app.route('/create-database', methods=['POST'])
def create_database():
    data = request.json
    db_name = data['name']
    query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
    execute_query(query, fetch=False)
    return '', 204

@app.route('/get-databases', methods=['GET'])
def get_databases():
    query = "SHOW DATABASES"
    result = execute_query(query)
    databases = [row['Database'] for row in result]
    #print(databases)
    not_allowed=['mysql','performance_schema','sys','information_schema']
    filtered_databases = []
    for i in databases:
        if i not in not_allowed:
            filtered_databases.append(i)
    
    #print(filtered_databases)
    return jsonify(databases=filtered_databases)
    #return jsonify(databases=databases)



@app.route('/switch-database', methods=['POST'])
def switch_database():
    data = request.json
    db_name = data['name']
    global db_config
    print('Switching from:',db_config['database'])
    db_config['database'] = db_name
    print("Connected to database:", db_config['database'])
    execute_query('''CREATE TABLE IF NOT EXISTS movies1 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            year INT,
            rating FLOAT,
            my_rating FLOAT,
            directors TEXT,
            cast TEXT,
            genres TEXT,
            runtime TEXT,
            languages TEXT,
            release_date TEXT,
            UNIQUE(title, year),
            path varchar(500),
            last_modified text
        )''')
    execute_query('''CREATE TABLE IF NOT exists backup like movies1''')
    if db_name in ['current','tamil']:
        create_temp_lang_files()
    return '', 204

@app.route('/delete-database-whole', methods=['POST'])
def delete_database_whole():
    data = request.json
    db_name = data['name']
    query = f"DROP DATABASE IF EXISTS {db_name}"
    execute_query(query, fetch=False)
    return '', 204

def get_current_ist_time():
    now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
    now_ist = now_utc.astimezone(pytz.timezone('Asia/Kolkata'))
    formatted_ist = now_ist.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_ist

@app.route('/')
def home():
    return render_template('base.html')

@app.route('/fetch_total_movies')
def fetch_total_movies():
    # Fetch the total runtime excluding 'temp_file' entries
    tot = execute_query("SELECT SUM(runtime) AS total_runtime FROM movies1 WHERE title <> %s", ('temp_file',))
    total_runtime = tot[0]['total_runtime'] if tot[0]['total_runtime'] is not None else 0

    # Fetch all movies excluding those with title 'temp_file'
    movies = execute_query("SELECT * FROM movies1 WHERE title <> %s order by last_modified desc", ('temp_file',))
    for movie in movies:
        for key in movie.keys():
            if not movie[key]:
                movie[key] = 'N/A'

    # Calculate the page runtime excluding 'temp_file' entries
    page_runtime = sum(int(movie['runtime']) for movie in movies if movie['runtime'] != 'N/A')

    # Count all movies excluding 'temp_file'
    total_count_query = "SELECT COUNT(*) AS total_count FROM movies1 WHERE title <> %s"
    total_count = execute_query(total_count_query, ('temp_file',))
    tm = total_count[0]['total_count']

    return jsonify({
        'all_tot_count': tm,
        'all_tot_runtime': total_runtime,
        'all_pg_runtime': page_runtime,
        'allmovies': movies
    })
# Your existing code for reading the welcome image or other default content


# Function to execute SQL queries with a retry mechanism
def execute_query(query, params=None, fetch=True, retries=5, connection=None):
    attempt = 0
    own_connection = False  # Indicator whether the connection was created inside the function
    cursor=None
    while attempt < retries:
        try:
            if connection is None:
                connection = mysql.connector.connect(**db_config)
                own_connection = True  # This connection is managed by this function
            cursor = connection.cursor(dictionary=True)
            #print('Executed query ',query,'with parameters: ',params,' \n\nin database: ',connection.database)
            cursor.execute(query, params)
            result = cursor.fetchall() if fetch else None
            if own_connection:
                connection.commit()  # Commit changes if this function created the connection
            cursor.close()
            if own_connection:
                connection.close()  # Close connection if it was created in this function
            return result
        except errors.DatabaseError as e:
            if e.errno == 1205:  # Lock wait timeout exceeded
                attempt += 1
                time.sleep(2)  # Wait before retrying
                if attempt == retries:
                    raise Exception(f"Failed to execute query after {retries} attempts") from e
            else:
                raise
	
        finally:
            if cursor is not None:
                cursor.close()
            if own_connection and connection is not None:
                connection.close()
	
# Initialize the IMDbPY instance
ia = IMDb()

def read_video_files(folder, include_subdirectories=True):
    video_files = []
    for root, dirs, files in os.walk(folder):
        if not include_subdirectories and root != folder:
            continue
        for file in files:
            if file.endswith(('.mp4','.MP4','.Mp4','.MKV','.Mkv', '.mkv','.AVI','.Avi', '.avi','.BC!','.Bc!','.bc!','.RMVB','.Rmvb','.rmvb','.dat','.Dat','.DAT','.vob','.VOB','.Vob','.flv','.FLV','.Flv','.mpeg','.MPEG','.Mpeg','.mpg','.MPG','.Mpg','.wmv','.WMV','.Wmv')):
                video_files.append(os.path.abspath(os.path.join(root, file)))
    return video_files

def rename_files(directory):
    pattern = re.compile(r"www\.[\w.-]+\s+-\s+(.+?)(\s+|\()(\d{4})(\s+|.*?-.*)")
    video_extensions = ('.mp4', '.MP4', '.Mp4', '.MKV', '.Mkv', '.mkv', 
                        '.AVI', '.Avi', '.avi', '.BC!', '.Bc!', '.bc!', 
                        '.RMVB', '.Rmvb', '.rmvb', '.dat', '.Dat', '.DAT', 
                        '.vob', '.VOB', '.Vob', '.flv', '.FLV', '.Flv', 
                        '.mpeg', '.MPEG', '.Mpeg', '.mpg', '.MPG', '.Mpg', 
                        '.wmv', '.WMV', '.Wmv')

    renamed_files = []
    for filename in os.listdir(directory):
        if filename.endswith(video_extensions):
            match = pattern.match(filename)
            if match:
                movie_name = match.group(1).strip()
                movie_year = match.group(3).strip()
                file_extension = os.path.splitext(filename)[1]
                new_filename = f"{movie_name} ({movie_year}){file_extension}"
                old_file = os.path.join(directory, filename)
                new_file = os.path.join(directory, new_filename)
                os.rename(old_file, new_file)
                renamed_files.append((filename, new_filename))
    return renamed_files

@app.route('/rename-files', methods=['POST'])
def rename_files_route():
    data = request.get_json()
    folder = data.get('folder')
    if not folder or not os.path.isdir(folder):
        return jsonify({"success": False, "error": "Invalid folder path"})
    try:
        renamed_files = rename_files(folder)
        return jsonify({"success": True, "renamed_files": renamed_files})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    
@app.route('/missing-languages')
def missing_languages():
    exclusion_clause = " AND title <> 'temp_file'"
    query = f"""
        SELECT id, last_modified, rating, my_rating, title, directors, cast, year, runtime, path FROM movies1
        WHERE COALESCE(languages, '') NOT IN ("Tamil", "English", "Hindi", "Telugu", "World", "Kuppai", "Malayalam", "To See", "Kannada")
        {exclusion_clause}
        ORDER BY last_modified DESC
    """
    movies = execute_query(query)
    
    for i in movies:
        for j in i.keys():
            if not i[j]:
                i[j] = 'N/A'

    total_runtime_query = "SELECT SUM(runtime) AS total_runtime FROM movies1 WHERE COALESCE(languages, '') NOT IN (%s) " + exclusion_clause
    total_runtime = execute_query(total_runtime_query, ("Tamil,English,Hindi,Telugu,World,Kuppai,Malayalam,To See,Kannada",))[0]['total_runtime'] or 0

    page_runtime = sum(int(movie['runtime']) for movie in movies if movie['runtime'] != 'N/A')

    total_count_query = "SELECT COUNT(*) AS total_count FROM movies1 WHERE COALESCE(languages, '') NOT IN (%s) " + exclusion_clause
    total_count = execute_query(total_count_query, ("Tamil,English,Hindi,Telugu,World,Kuppai,Malayalam,To See,Kannada",))[0]['total_count']

    return jsonify({
        'movies': movies,
        'total_count': total_count,
        'total_runtime': total_runtime,
        'page_runtime': page_runtime
    })


@app.route('/missing-genres')
def missing_genres():
    exclusion_clause = " AND title <> 'temp_file'"
    query = f"""
        SELECT id, last_modified, rating, my_rating, title, directors, cast, year, runtime, path FROM movies1
        WHERE genres="" or genres is null
        {exclusion_clause}
        ORDER BY last_modified DESC
    """
    movies = execute_query(query)
    
    for i in movies:
        for j in i.keys():
            if not i[j]:
                i[j] = 'N/A'

    total_runtime_query = "SELECT SUM(runtime) AS total_runtime FROM movies1 WHERE COALESCE(languages, '') NOT IN (%s) " + exclusion_clause
    total_runtime = execute_query(total_runtime_query, ("Tamil,English,Hindi,Telugu,World,Kuppai,Malayalam,To See,Kannada",))[0]['total_runtime'] or 0

    page_runtime = sum(int(movie['runtime']) for movie in movies if movie['runtime'] != 'N/A')

    total_count_query = "SELECT COUNT(*) AS total_count FROM movies1 WHERE COALESCE(languages, '') NOT IN (%s) " + exclusion_clause
    total_count = execute_query(total_count_query, ("Tamil,English,Hindi,Telugu,World,Kuppai,Malayalam,To See,Kannada",))[0]['total_count']

    return jsonify({
        'movies': movies,
        'total_count': total_count,
        'total_runtime': total_runtime,
        'page_runtime': page_runtime
    })





# Function to extract movie title and year from the file name
def extract_movie_info(filename):
    patterns = [
        r"(?:\d+\.\d+\s*)?(?:HD|HQ)\s*-\s*(.+?)\s*\((\d{4})\)",  # Rating - HD/HQ - Title (Year)
        r"(?:\d+\.\d+\s*-\s*)?(.+?)\s*\((\d{4})\)",              # Rating - Title (Year)
        r"(?:HD|HQ)\s*-\s*(.+?)\s*\((\d{4})\)",                  # HD/HQ - Title (Year)
        r"(.+?)\s*\((\d{4})\)",                                  # Title (Year)
        r"(.+?)[\W_](\d{4})",                                    # Title_Year
        r"HD - \[?(\d{4})\]?\s*(.+?)\s*\((\d{4})\)\s*TML",       # HD - [2022] Title (2023) TML
        r"HD - \[?(\d{4})\]?\s*(.+?)\s*\((\d{4})\)",             # HD - [2022] Title (2023)
        r"HD - \[?(\d{4})\]?\s*(.+?)\s*TML",                     # HD - [2022] Title TML
        r"HD - \[?(\d{4})\]?\s*(.+)",                            # HD - [2022] Title
        r"HD - (.+?)\s*\((\d{4})\)\s*TML",                       # HD - Title (2023) TML
        r"HD - (.+?)\s*\((\d{4})\)",                             # HD - Title (2023)
        r"HD - (.+?)\s*TML",                                     # HD - Title TML
        r"HD - (.+)",                                            # HD - Title
        r"(.+)"
    ]
    
    for pattern in patterns:
        match = re.match(pattern, filename)
        if match:
            if pattern in {patterns[0], patterns[1], patterns[2], patterns[3], patterns[4]}:
                title = match.group(1).strip()
                year = match.group(2).strip() if len(match.groups()) > 1 else None
                return title, year
            elif pattern in {patterns[5], patterns[6], patterns[7], patterns[8], patterns[9], patterns[10]}:
                if len(match.groups()) == 3:
                    year1 = match.group(1).strip()
                    title = match.group(2).strip()
                    year2 = match.group(3).strip()
                    return title, year2  # Use year2 as the final year
                elif len(match.groups()) == 2:
                    year1 = match.group(1).strip()
                    title = match.group(2).strip()
                    return title, year1  # Use year1 as the final year
                else:
                    title = match.group(1).strip()
                    year = match.group(2).strip()
                    return title, year
            elif pattern in {patterns[11], patterns[12], patterns[13]}:
                title = match.group(1).strip()
                return title, None
            else:
                title = match.group(1).strip()
                return title, None

    return None, None


# Function to fetch movie information from IMDb
def get_movie_info(movie_title, movie_year=None):
    try:
        movies = ia.search_movie(movie_title)
        if not movies:
            print(f"No results found for: {movie_title}")
            return None
            
        for movie in movies:
            # Try to match by year if provided
            if movie_year and 'year' in movie and str(movie['year']) == str(movie_year):
                print(f"Found exact match for {movie_title} ({movie_year})")
                return ia.get_movie(movie.movieID)
            
            # Try to match by title similarity
            if movie['title'].lower() == movie_title.lower():
                print(f"Found title match for {movie_title}")
                return ia.get_movie(movie.movieID)
        
        # If no exact match, return the first result
        print(f"Using first result for: {movie_title}")
        return ia.get_movie(movies[0].movieID)
        
    except Exception as e:
        print(f"Error fetching information for {movie_title} ({movie_year}): {e}")
        return None


@app.route('/languages')
def get_languages():
    check_table_query = """
    SELECT COUNT(*)
    FROM information_schema.tables 
    WHERE table_schema = DATABASE() AND table_name = 'movies1'
    """
    table_exists = execute_query(check_table_query)[0]['COUNT(*)'] > 0
    
    if not table_exists:
        return jsonify([])

    # Check if 'my_rating' column exists
    check_column_query = """
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'movies1' AND column_name = 'my_rating'
    """
    column_exists = execute_query(check_column_query)[0]['COUNT(*)'] > 0
    
    # If 'my_rating' column does not exist, add it next to 'rating' column
    if not column_exists:
        add_column_query = """
        ALTER TABLE movies1
        ADD COLUMN my_rating FLOAT AFTER rating
        """
        execute_query(add_column_query, fetch=False)

    query = "SELECT DISTINCT languages FROM movies1"
    languages = execute_query(query)
    language_set = set()
    others_set = set()
    valid_languages = {'Tamil', 'English', 'Hindi', 'Malayalam', 'Telugu', 'Kannada', 'World','To See','Kuppai'}
    
    for lang in languages:
        lang_list = [language.strip() for language in lang['languages'].split(',')]
        for language in lang_list:
            if language in valid_languages:
                language_set.add(language)
            else:
                others_set.add(language)
    
    if others_set:
        language_set.add('Others')
    
    language_list = sorted(list(language_set), key=lambda x: ('Tamil' not in x, 'English' not in x, 'Hindi' not in x,
                                                              'Malayalam' not in x, 'Telugu' not in x,'Kannada' not in x,'World' not in x, 'Others' not in x, 'To See' not in x,'Kuppai' not in x,x))
    
    return jsonify(language_list)


# Route to get genres for a specific language
@app.route('/genres/<language>')
def get_genres(language):
    if language == 'Others':
        query = """
        SELECT DISTINCT genres 
        FROM movies1 
        WHERE languages NOT LIKE '%Tamil%' 
        AND languages NOT LIKE '%English%' 
        AND languages NOT LIKE '%Hindi%' 
        AND languages NOT LIKE '%Malayalam%' 
        AND languages NOT LIKE '%Telugu%'
        AND languages NOT LIKE '%Kannada%'
        AND languages NOT LIKE '%World%'
        """
        genres = execute_query(query)
    else:
        query = "SELECT DISTINCT genres FROM movies1 WHERE languages LIKE %s"
        genres = execute_query(query, (f"%{language}%",))

    genre_set = set()
    for genre in genres:
        if genre and (genre['genres']=='' or not genre['genres']):
            genre_set.add('Not Availible')
        if genre and genre['genres']:
            cleaned_genres = [g.strip() for g in genre['genres'].split(',') if g.strip()]
            genre_set.update(cleaned_genres)

    # Add 'Not Availible' if there are no genres or genres are NULL
    if not genre_set:
        genre_set.add('Not Availible')

    # Sort genres alphabetically and remove duplicates
    genre_list = sorted(list(genre_set))
    #print(genre_list)
    return jsonify(genre_list)

@app.route('/add-genre', methods=['POST'])
def add_genre():
    data = request.json
    language = data['language']
    genre = data['genre']
    current_ist_time=get_current_ist_time()
    # Insert a temp_file entry into the movies table
    insert_temp_file_query = "INSERT INTO movies1(title, languages, genres,last_modified) VALUES (%s, %s, %s,%s)"
    execute_query(insert_temp_file_query, ('temp_file', language, genre,current_ist_time), fetch=False)

    return jsonify({'message': 'Genre added and temp_file entry created successfully'}), 201

@app.route('/delete-genre/<language>/<genre>', methods=['DELETE'])
def delete_genre(language, genre):
    try:
        # Check if there are movies with the given genre
        check_query = """
        SELECT COUNT(*) as movie_count
        FROM movies1
        WHERE languages like %s AND genres like %s
        """
        result = execute_query(check_query, (f"%{language}%", f"%{genre}%"))
        if result[0]['movie_count'] > 0:
            return jsonify({'hasMovies': True}), 400
        
        return jsonify({'success': True})
    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500
    
# Route to get movies for a specific language and genre
@app.route('/movies/<language>/<genre>')
def get_movies(language, genre):
    tot = execute_query("SELECT SUM(runtime) AS total_runtime FROM movies1 WHERE title <> %s", ('temp_movies',))
    total_runtime = tot[0]['total_runtime'] if tot[0]['total_runtime'] is not None else 0
    movies=[]
    exclusion_clause = " AND title <> 'temp_file'"

    if language == 'Others':
        if genre.lower() == 'all':
            query = f'''SELECT id, last_modified, rating, my_rating, title, directors, cast, year, runtime, path FROM movies1 
                        WHERE languages NOT LIKE '%Tamil%' 
                        AND languages NOT LIKE '%English%' 
                        AND languages NOT LIKE '%Hindi%' 
                        AND languages NOT LIKE '%Malayalam%' 
                        AND languages NOT LIKE '%Telugu%'
                        AND languages NOT LIKE '%Kannada%'
                        AND languages NOT LIKE '%World%' 
                        AND languages NOT LIKE '%To See%' 
                        AND languages NOT LIKE '%Kuppai%' {exclusion_clause}
                        ORDER BY last_modified DESC'''
        elif genre.lower() == 'not available':
            query = f'''SELECT id, last_modified, rating, my_rating, title, directors, cast, year, runtime, path FROM movies1 
                        WHERE genres IS NULL 
                        AND languages NOT LIKE '%Tamil%' 
                        AND languages NOT LIKE '%English%' 
                        AND languages NOT LIKE '%Hindi%' 
                        AND languages NOT LIKE '%Malayalam%' 
                        AND languages NOT LIKE '%Telugu%'
                        AND languages NOT LIKE '%Kannada%'
                        AND languages NOT LIKE '%World%' 
                        AND languages NOT LIKE '%To See%' 
                        AND languages NOT LIKE '%Kuppai%' {exclusion_clause}
                        ORDER BY last_modified DESC'''
        else:
            query = f'''SELECT id, last_modified, rating, my_rating, title, directors, cast, year, runtime, path FROM movies1 
                        WHERE languages NOT LIKE '%Tamil%' 
                        AND languages NOT LIKE '%English%' 
                        AND languages NOT LIKE '%Hindi%' 
                        AND languages NOT LIKE '%Malayalam%' 
                        AND languages NOT LIKE '%Telugu%' 
                        AND languages NOT LIKE '%Kannada%'
                        AND languages NOT LIKE '%World%'
                        AND languages NOT LIKE '%To See%' 
                        AND languages NOT LIKE '%Kuppai%' AND genres LIKE %s {exclusion_clause}
                        ORDER BY last_modified DESC'''
            movies = execute_query(query, (f"%{genre}%",))
    else:
        if genre.lower() == 'all':
            query = f"SELECT id, last_modified, rating, my_rating, title, directors, cast, year, runtime, path FROM movies1 WHERE languages LIKE %s {exclusion_clause} ORDER BY last_modified DESC"
            movies = execute_query(query, (f"%{language}%",))
        else:
            query = f"SELECT id, last_modified, rating, my_rating, title, directors, cast, year, runtime, path FROM movies1 WHERE languages LIKE %s AND genres LIKE %s {exclusion_clause} ORDER BY last_modified DESC"
            movies = execute_query(query, (f"%{language}%", f"%{genre}%"))

    for i in movies:
        for j in i.keys():
            if not i[j]:
                i[j] = 'N/A'

    page_runtime = sum(int(movie['runtime']) for movie in movies if movie['runtime'] != 'N/A')

    total_count_query = "SELECT COUNT(*) FROM movies1 WHERE title <> %s"
    total_count = execute_query(total_count_query, ('temp_file',))
    tm = total_count[0]['COUNT(*)']

    return jsonify({'movies': movies, 'total_count': tm, 'total_runtime': total_runtime, 'page_runtime': page_runtime})

def create_temp_lang_files():
    # First, delete all entries with title 'temp_file'
    delete_query = "DELETE FROM movies1 WHERE title = %s"
    execute_query(delete_query, ('temp_file',), fetch=False)

    # Define valid languages
    valid_languages = ['Tamil', 'English', 'Hindi', 'Malayalam', 'Telugu', 'Kannada', 'World', 'To See', 'Kuppai']
    for lang in valid_languages:
        res = execute_query("SELECT * FROM movies1 WHERE languages LIKE %s", (f"%{lang}%",))
        if not res:
            current_ist_time = get_current_ist_time()
            insert_temp_file_query = "INSERT INTO movies1 (title, languages, genres, last_modified) VALUES (%s, %s, %s, %s)"
            execute_query(insert_temp_file_query, ('temp_file', lang, 'Action,Adventure,Comedy,Crime,Drama,History,Horror,Thriller,Romance,War', current_ist_time), fetch=False)

@app.route('/update-database', methods=['POST'])
def update_database():
    data = request.get_json()
    folder_path = data.get('folder_path', 'G:/00 English Latest')
    include_subdirectories = data.get('search_subdirectories', True)
    
    # Assuming this function retrieves a list of movies from the specified folder
    movies = read_video_files(folder_path, include_subdirectories)
    total_files = len(movies)
    
    # Create temporary files in movies1 table for valid languages
    original_database = db_config['database']
    print(original_database)
    if original_database in ['current','tamil']:
        create_temp_lang_files()
    
    return jsonify({"total_files": total_files})

@app.route('/update-database-stream', methods=['GET'])
def update_database_stream():
    @stream_with_context
    def generate_progress():
        db = mysql.connector.connect(**db_config)
        cursor = db.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS movies1 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            year INT,
            rating FLOAT,
            my_rating FLOAT,
            directors TEXT,
            cast TEXT,
            genres TEXT,
            runtime TEXT,
            languages TEXT,
            release_date TEXT,
            UNIQUE(title, year),
            path varchar(500),
            last_modified text
        )''')
        
        processed_files = 0
        folder_path = request.args.get('folder_path', 'G:/00 English Latest')
        include_subdirectories = request.args.get('search_subdirectories', 'true') == 'true'
        movies = read_video_files(folder_path, include_subdirectories)
        total_files = len(movies)  # Get total number of files
        print('Accessing folder:',folder_path)  
        for movie in movies:
            print("Movie path :", movie)
            title, year = extract_movie_info(os.path.splitext(os.path.basename(movie))[0])
            title=title.capitalize()
            print("Title - {}, Year - {}".format(title, year))
            #print(type(title))
            current_ist_time = get_current_ist_time()
            if title:
                cursor.execute("SELECT 1 FROM movies1 WHERE title = %s", (title,))
                result = cursor.fetchall()
                if not result:
                    movie_info = get_movie_info(title, year)
                    if movie_info:
                        im_title = movie_info.get('title')
                        im_year = movie_info.get('year')
                        cursor.execute("SELECT 1 FROM movies1 WHERE title = %s AND (year = %s OR year IS NULL)", (im_title, im_year))
                        result = cursor.fetchall()
                        if not result:
                            languages = ', '.join(movie_info.get('languages', []))
                            valid_languages = {'Tamil', 'English', 'Hindi', 'Malayalam', 'Telugu','Kannada','World'}
                            if not any(lang in valid_languages for lang in languages.split(',')):
                                languages = 'Others'
                            cursor.execute('''
                                INSERT INTO movies1 (title, year, rating, directors, cast, genres, runtime, languages, release_date, path,last_modified)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
                            ''', (
                                movie_info.get('title'),
                                movie_info.get('year'),
                                movie_info.get('rating'),
                                ', '.join(director['name'] for director in movie_info.get('directors', [])),
                                ', '.join(actor['name'] for actor in movie_info.get('cast', [])[:5]),
                                ', '.join(movie_info.get('genres', [])),
                                movie_info.get('runtime', [''])[0],
                                ', '.join(movie_info.get('languages', [])),
                                movie_info.get('original air date'),
                                movie,
                                current_ist_time
                            ))
                            db.commit()
                        else:
                            path_m=execute_query("Select path from movies1 where title = %s",(im_title,))
                            pre_path=path_m[0]
                            dup_path=pre_path['path']
                            print()
                            print("{} Movie already present in path {}".format(title,dup_path))
                            print()

                    else:
                        cursor.execute('''insert into movies1(title, year, rating, directors, cast, genres, runtime, languages, release_date, path,last_modified) 
                                       values (%s,%s,NULL,NULL,NULL,NULL,NULL,%s,NULL,%s,%s)''',(title,year,'Not Availible',movie,current_ist_time))
                        db.commit()

                else:
                    path_m=execute_query("Select path from movies1 where title = %s",(title,))
                    pre_path=path_m[0]
                    dup_path=pre_path['path']
                    print()
                    print("{} Movie already present in path {}".format(title,dup_path))
                    print()
                # Send the progress data to the client
                processed_files += 1
                progress_data = {
                    'total_files': total_files,
                    'processed_files': processed_files
                }

                # Yield the updated progress data
                yield f"data: {json.dumps(progress_data)}\n\n"

        db.close()
        
        yield f"data: {json.dumps({'message': 'Database updated successfully', 'total_files': total_files, 'processed_files': processed_files})}\n\n"
    return Response(generate_progress(), content_type='text/event-stream')


@app.route('/get-files', methods=['GET'])
def get_files():
    folder_path = request.args.get('folder_path', 'G:/00 English Latest')
    search_subdirectories = request.args.get('subdirectories', 'false').lower() == 'true'
    if search_subdirectories:
        files = [os.path.relpath(os.path.join(root, file), folder_path)
                 for root, dirs, files in os.walk(folder_path)
                 for file in files if file.endswith(('.mp4', '.MP4', '.Mp4', '.MKV', '.Mkv', '.mkv', '.AVI', '.Avi', '.avi', '.BC!', '.Bc!', '.bc!', '.RMVB', '.Rmvb', '.rmvb', '.dat', '.Dat', '.DAT', '.vob', '.VOB', '.Vob', '.flv', '.FLV', '.Flv', '.mpeg', '.MPEG', '.Mpeg', '.mpg', '.MPG', '.Mpg', '.wmv', '.WMV', '.Wmv'))]
    else:
        files = [file for file in os.listdir(folder_path)
                 if os.path.isfile(os.path.join(folder_path, file)) and file.endswith(('.mp4', '.MP4', '.Mp4', '.MKV', '.Mkv', '.mkv', '.AVI', '.Avi', '.avi', '.BC!', '.Bc!', '.bc!', '.RMVB', '.Rmvb', '.rmvb', '.dat', '.Dat', '.DAT', '.vob', '.VOB', '.Vob', '.flv', '.FLV', '.Flv', '.mpeg', '.MPEG', '.Mpeg', '.mpg', '.MPG', '.Mpg', '.wmv', '.WMV', '.Wmv'))]
    return jsonify({'files': files})

@app.route('/update-selected-files', methods=['POST'])
def update_selected_files():
    data = request.get_json()
    selected_files = data.get('files', [])
    total_files = len(selected_files)
    res=execute_query("Select * from movies1 where languages like '%World%'")
    if not len(res):
        current_ist_time = get_current_ist_time()
        insert_temp_file_query = "INSERT INTO movies1(title, languages,genres, last_modified) VALUES (%s, %s, %s,%s)"
        execute_query(insert_temp_file_query, ('temp_file', 'World', 'Action,Adventure,Comedy,Crime,Drama,History,Horror,Thriller,Romance,War',current_ist_time), fetch=False)
    return jsonify({"total_files": total_files})

@app.route('/update-selected-files-stream', methods=['GET'])
def update_selected_files_stream():
    selected_files = request.args.get('files', '').split(',')
    user_provided_path = request.args.get('path', 'G:/00 English Latest')  # Default to 'G:/00 English Latest' if not provided
    #print(user_provided_path)
    @stream_with_context
    def generate_progress():
        db = mysql.connector.connect(**db_config)
        cursor = db.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS movies1 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            year INT,
            rating FLOAT,
            my_rating FLOAT,
            directors TEXT,
            cast TEXT,
            genres TEXT,
            runtime TEXT,
            languages TEXT,
            release_date TEXT,
            UNIQUE(title, year),
            path varchar(500),
            last_modified text
        )''')
        processed_files = 0
        total_files = len(selected_files)
        for movie in selected_files:
            movie_path = os.path.join(user_provided_path, movie).replace('\\', '/')  # Use the user-provided path here
            #print(movie_path)
            title, year = extract_movie_info(os.path.splitext(os.path.basename(movie_path))[0])
            #print(title,',',len(title))            
            title = title.capitalize()
            print(f"Processing file: {movie}")
            print(f"Extracted title: {title}, year: {year}")

            movie_info = get_movie_info(title, year)
            if movie_info:
                print(f"Found movie info: {movie_info.get('title')} ({movie_info.get('year')})")
            else:
                print("No movie info found - will store as N/A")
            current_ist_time = get_current_ist_time()
            if title:
                cursor.execute("SELECT 1 FROM movies1 WHERE title = %s", (title,))
                result = cursor.fetchall()
                if not result:
                    movie_info = get_movie_info(title, year)
                    if movie_info:
                        im_title = movie_info.get('title')
                        #print(im_title,',',len(im_title))         
                        im_year = movie_info.get('year')
                        cursor.execute("SELECT 1 FROM movies1 WHERE title = %s AND (year = %s OR year IS NULL)", (im_title, im_year))
                        result = cursor.fetchall()
                        print(not result)
                        if not result:
                            languages = ', '.join(movie_info.get('languages', []))
                            valid_languages = {'Tamil', 'English', 'Hindi', 'Malayalam', 'Telugu','Kannada','World'}
                            if not any(lang in valid_languages for lang in languages.split(',')):
                                languages = 'Others'
                            cursor.execute('''
                                INSERT INTO movies1 (title, year, rating, directors, cast, genres, runtime, languages, release_date, path, last_modified)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ''', (
                                movie_info.get('title'),
                                movie_info.get('year'),
                                movie_info.get('rating'),
                                ', '.join(director['name'] for director in movie_info.get('directors', [])),
                                ', '.join(actor['name'] for actor in movie_info.get('cast', [])[:5]),
                                ', '.join(movie_info.get('genres', [])),
                                movie_info.get('runtime', [''])[0],
                                ', '.join(movie_info.get('languages', [])),
                                movie_info.get('original air date'),
                                movie_path,
                                current_ist_time
                            ))
                            db.commit()
                        else:
                            path_m = execute_query("SELECT path FROM movies1 WHERE title = %s", (im_title,))
                            pre_path = path_m[0]
                            dup_path = pre_path['path']
                            print(f"{title} Movie already present in path {dup_path}")

                    else:
                        cursor.execute('''
                            INSERT INTO movies1 (title, year, rating, directors, cast, genres, runtime, languages, release_date, path)
                            VALUES (%s, %s, NULL, NULL, NULL, NULL, NULL, 'Not Available', NULL, %s)
                        ''', (title, year, movie_path))
                        db.commit()

                else:
                    path_m = execute_query("SELECT path FROM movies1 WHERE title = %s", (title,))
                    pre_path = path_m[0]
                    dup_path = pre_path['path']
                    print(f"{title} Movie already present in path {dup_path}")

                # Send the progress data to the client
                processed_files += 1
                progress_data = {
                    'total_files': total_files,
                    'processed_files': processed_files
                }

                # Yield the updated progress data
                yield f"data: {json.dumps(progress_data)}\n\n"

        db.close()
        
        yield f"data: {json.dumps({'message': 'Selected files updated successfully', 'total_files': total_files, 'processed_files': processed_files})}\n\n"

    return Response(generate_progress(), content_type='text/event-stream')

@app.route('/delete-database', methods=['POST'])
def delete_database():
    try:
        query = f"TRUNCATE TABLE movies1"
        execute_query(query)
        return '', 200
    except Exception as e:
        print(f"Error deleting database: {e}")
        return str(e), 500

@app.route('/search-movies/<attribute>/<query>')
def search_movies(attribute, query):
    attribute = attribute.strip()
    query = query.strip()
    query = query.title()
    tot=execute_query("Select sum(runtime) from movies1")
    tota=tot[0]
    total_runtime=tota['sum(runtime)']
    #print(total_runtime)
    print(attribute)
    #print(query)
    if query == "Not Found":
        if attribute == 'actor':
            sql_query = "SELECT * FROM movies1 WHERE cast IS NULL"
        elif attribute == 'director':
            sql_query = "SELECT * FROM movies1 WHERE directors IS NULL"
        else:
            sql_query = f"SELECT * FROM movies1 WHERE {attribute} IS NULL"
        params = None
    else:
        if attribute == 'rating':
            min_rating, max_rating = map(float, query.split('-'))
            sql_query = "SELECT * FROM movies1 WHERE rating BETWEEN %s AND %s"
            params = (min_rating, max_rating)
        elif attribute == 'my_rating':
            min_rating, max_rating = map(float, query.split('-'))
            sql_query = "SELECT * FROM movies1 WHERE my_rating BETWEEN %s AND %s"
            params = (min_rating, max_rating)
        elif attribute == 'runtime':
            min_runtime, max_runtime = map(int, query.split('-'))
            sql_query = "SELECT * FROM movies1 WHERE runtime BETWEEN %s AND %s"
            params = (min_runtime, max_runtime)
        elif attribute == 'actor':
            sql_query = "SELECT * FROM movies1 WHERE cast LIKE %s"
            params = (f"%{query}%",)
        elif attribute == 'path_root':
            sql_query = "SELECT * FROM movies1 WHERE path LIKE %s"
            params = (f"%{query}%",)
        elif attribute == 'genre':  # Added genre search logic
            sql_query = "SELECT * FROM movies1 WHERE genres LIKE %s"
            params = (f"%{query}%",)
        else:
            sql_query = f"SELECT * FROM movies1 WHERE {attribute} LIKE %s"
            params = (f"%{query}%",)
    
    mlist = execute_query(sql_query, params)
    #print(mlist)
    if attribute == 'path_root':
        filtered_mlist = []
        for item in mlist:
            relative_path = item['path'].split(query)[-1]
            if ':' in query:
                if relative_path.count(os.path.sep) <= 1:  # count number of separators in the remaining path
                    filtered_mlist.append(item)
            else:
                if relative_path.count(os.path.sep) <= 2:  # count number of separators in the remaining path
                    filtered_mlist.append(item)
        mlist = filtered_mlist

    page_runtime=0
    for i in mlist:
        #print(i['runtime'])
        if i['runtime'] and i['runtime']!='N/A' and i['runtime']!='':
            page_runtime+=int(i['runtime'])
    for i in mlist:
        for j in i.keys():
            if not i[j]:
                i[j]='N/A'
    total_count_query = "SELECT COUNT(*) FROM movies1"
    total_count = execute_query(total_count_query)
    tot_mov=total_count[0]
    tm=tot_mov['COUNT(*)']
    return jsonify({'movies': mlist, 'total_count': tm,'total_runtime':total_runtime,'page_runtime':page_runtime})

@app.route('/get-options/<attribute>')
def get_options(attribute):
    attribute = attribute.strip()
    if attribute == 'title':
        sql_query = "SELECT DISTINCT title FROM movies1"
    elif attribute == 'actor':
        sql_query = "SELECT DISTINCT cast AS actor FROM movies1"
    elif attribute == 'directors':
        sql_query = "SELECT DISTINCT directors FROM movies1"
    elif attribute == 'rating':
        options = ["4-5", "5-6", "6-7", "7-8", "8-9", "9-10", "Not Found"]
        return jsonify(options)
    elif attribute == 'my_rating':
        options = ["4-5", "5-6", "6-7", "7-8", "8-9", "9-10", "Not Found"]
        return jsonify(options)
    elif attribute == 'year':
        sql_query = "SELECT DISTINCT year FROM movies1"
    elif attribute == 'path':
        sql_query = "select distinct path from movies1"
    elif attribute == 'runtime':
        options = ["50-100", "100-150", "150-200", "200-250", "Not Found"]
        return jsonify(options)
    elif attribute == 'genre':  # Added genre option
        sql_query = "SELECT DISTINCT genres FROM movies1"
        results = execute_query(sql_query)
        genres = set()
        for result in results:
            if result['genres']:
                genre_list = result['genres'].split(',')
                genres.update([genre.strip() for genre in genre_list])
        options = sorted(genres)
        options.append("Not Found")
        return jsonify(options)
    else:
        return jsonify([])

    results = execute_query(sql_query)
    options = []
    for result in results:
        value = result[attribute]
        if value:
            if isinstance(value, str):
                options.append(value.strip())
            else:
                options.append(str(value))
    options.append("Not Found")
    options = sorted(set(options))

    return jsonify(options)

@app.route('/adv-search-movies/<path:search_params>')
def adv_search_movies(search_params):
    pairs = search_params.split(';')  # Split by semicolon to get individual pairs
    queries = []
    params = []
    tot=execute_query("Select sum(runtime) from movies1")
    tota=tot[0]
    total_runtime=tota['sum(runtime)']
    #print(total_runtime)
    for pair in pairs:
        attribute, query = pair.split('/')
        if query == "Not Found":
            if attribute == 'actor':
                queries.append("cast IS NULL")
            elif attribute == 'directors':
                queries.append("directors IS NULL")
            else:
                queries.append(f"{attribute} IS NULL")
        else:
            if attribute == 'rating':
                min_rating, max_rating = map(float, query.split('-'))
                queries.append("rating BETWEEN %s AND %s")
                params.extend([min_rating, max_rating])
            elif attribute == 'my_rating':
                min_rating, max_rating = map(float, query.split('-'))
                queries.append("my_rating BETWEEN %s AND %s")
                params.extend([min_rating, max_rating])
            elif attribute == 'runtime':
                min_runtime, max_runtime = map(int, query.split('-'))
                queries.append("runtime BETWEEN %s AND %s")
                params.extend([min_runtime, max_runtime])
            elif attribute == 'actor':
                queries.append("cast LIKE %s")
                params.append(f"%{query}%")
            elif attribute == 'genre':
                queries.append("genres LIKE %s")
                params.append(f"%{query}%")
            else:
                queries.append(f"{attribute} LIKE %s")
                params.append(f"%{query}%")

    sql_query = f"SELECT * FROM movies1 WHERE {' AND '.join(queries)}"
    mlist = execute_query(sql_query, params)
    page_runtime=0
    for i in mlist:
        #print(i['runtime'])
        if i['runtime'] and i['runtime']!='N/A' and i['runtime']!='':
            page_runtime+=int(i['runtime'])
    total_count_query = "SELECT COUNT(*) FROM movies1"
    total_count = execute_query(total_count_query)
    tot_mov = total_count[0]
    tm = tot_mov['COUNT(*)']
    
    return jsonify({'movies': mlist, 'total_count': tm,'total_runtime':total_runtime,'page_runtime':page_runtime})

@app.route('/create-languages', methods=['POST'])
def create_languages():
    data = request.json
    languages = data.get('languages', [])
    create_temp_files(languages)
    return jsonify({'message': 'Languages creation process completed.'})

@app.route('/delete-languages', methods=['POST'])
def delete_languages():
    data = request.json
    languages = data.get('languages', [])
    message = delete_temp_files(languages)
    return jsonify({'message': message})

def create_temp_files(languages):
    for lang in languages:
        res = execute_query("SELECT * FROM movies1 WHERE languages LIKE %s AND title <> 'temp_file'", (f"%{lang}%",))
        if not res:
            current_ist_time = get_current_ist_time()
            execute_query("INSERT INTO movies1 (title, languages, genres, last_modified) VALUES (%s, %s, %s, %s)",
                          ('temp_file', lang, 'Various', current_ist_time), fetch=False)

def delete_temp_files(languages):
    for lang in languages:
        res = execute_query("SELECT * FROM movies1 WHERE languages LIKE %s AND title = 'temp_file'", (f"%{lang}%",))
        if res:
            if len(res) == 1:  # Only temp file exists
                execute_query("DELETE FROM movies1 WHERE id = %s", (res[0]['id'],))
            else:
                return f"Movies are still present in {lang}, move them before deleting the language."
    return "Languages deletion process completed."

@app.route('/movie-details/<title>')
def movie_details(title):
    query = "SELECT title, cast, genres, release_date FROM movies1 WHERE title = %s"
    movie = execute_query(query, (title,))
    if movie:
        #print(movie)
        return jsonify(movie[0])
    else:
        return jsonify({"error": "Movie not found"}), 404

@app.route('/open-movie', methods=['POST'])
def open_movie():

    data = request.get_json()
    path = data.get('path')
    try:
        if os.path.exists(path):
            if os.name == 'nt':  # For Windows
                os.startfile(path)
            return jsonify({"message": "Movie opened successfully"}), 200
        else:
            return jsonify({"error": "File not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/move-movies', methods=['POST'])
def move_movies():
    data = request.json
    folder_path = data['folder_path']
    rating = data['rating']

    # Retrieve movies from the database with rating less than the provided rating
    query = "SELECT id,title, path FROM movies1 WHERE rating < %s"
    movies_to_move = execute_query(query, (rating,), fetch=True)

    # Move each movie to the destination folder and update the database
    total_movies = len(movies_to_move)
    moved_movies = 0
    for movie in movies_to_move:
        src_path = movie['path']
        dest_path = os.path.join(folder_path, os.path.basename(src_path))
        try:
            shutil.move(src_path, dest_path)
            # Update the database with the new path
            update_query = "UPDATE movies1 SET path = %s WHERE title = %s"
            execute_query(update_query, (dest_path, movie['title']), fetch=False)
            # Delete the moved movie from the database
            delete_query = "DELETE FROM movies1 WHERE id = %s"
            execute_query(delete_query, (movie['id'],), fetch=False)
            moved_movies += 1
        except Exception as e:
            print(f"Error moving movie '{movie['title']}': {e}")

    return jsonify({'total_movies': total_movies, 'moved_movies': moved_movies}), 200



@app.route('/move-movies-stream')
def move_movies_stream():
    
    rating = request.args.get('rating')
    @stream_with_context
    def generate():
        # Retrieve movies from the database with rating less than the provided rating
        query = "SELECT COUNT(*) FROM movies1 WHERE rating < %s"
        total_movies = execute_query(query, (rating,), fetch=True)[0]['COUNT(*)']
        moved_movies = 0

        for i in range(total_movies):
            # Simulate the process of moving a movie
            time.sleep(0.1)
            moved_movies += 1
            yield f"data: {json.dumps({'total_movies': total_movies, 'moved_movies': moved_movies})}\n\n"
            sys.stdout.flush()

        yield f"data: {json.dumps({'message': 'Movies moved successfully'})}\n\n"

    return Response(generate(), mimetype='text/event-stream')

@app.route('/move-meta', methods=['POST'])
def move_meta():
    data = request.get_json()
    language = data.get('language')
    genre = data.get('genre')
    movie_ids = data.get('movieIds')

    if not language or not movie_ids:
        return jsonify({'error': 'Missing data'}), 400

    movie_ids_tuple = tuple(movie_ids)
    placeholders = ', '.join(['%s'] * len(movie_ids_tuple))  # Create a string of placeholders
    current_ist_time=get_current_ist_time()
    if genre == 'Keep Genre':
        query = f"""
            UPDATE movies1
            SET languages = %s,last_modified=%s
            WHERE id IN ({placeholders})
        """
        params = (language,current_ist_time, *movie_ids_tuple)  # Combine language and movie_ids into one tuple
    else:
        query = f"""
            UPDATE movies1
            SET languages = %s, genres = %s,last_modified=%s
            WHERE id IN ({placeholders})
        """
        params = (language, genre,current_ist_time, *movie_ids_tuple)  # Combine language, genre, and movie_ids into one tuple

    execute_query(query, params)
    
    return jsonify({'success': True})

@app.route('/change-path-disk', methods=['POST'])
def change_path_disk():
    data = request.get_json()
    present_disk = data.get('presentDisk')
    required_disk = data.get('requiredDisk')

    if not present_disk or not required_disk:
        return jsonify({'error': 'Missing data'}), 400

    # Update the path in the database
    query = """
        UPDATE movies1
        SET path = REPLACE(path, %s, %s)
        WHERE path LIKE %s
    """
    params = (present_disk, required_disk, f"{present_disk}%")
    execute_query(query, params)
    
    return jsonify({'success': True})

@app.route('/delete-entries', methods=['POST'])
def delete_entries():
    data = request.get_json()
    movie_ids = data.get('movieIds')

    if not movie_ids:
        return jsonify({'error': 'No movie IDs provided'}), 400

    #print('Received movie IDs for deletion:', movie_ids)  # Debugging line

    movie_ids_tuple = tuple(movie_ids)
    placeholders = ', '.join(['%s'] * len(movie_ids_tuple))  # Create a string of placeholders
    query = f"DELETE FROM movies1 WHERE id IN ({placeholders})"
    params = (*movie_ids_tuple,)  # Ensure params is a tuple

    execute_query(query, params)
    
    return jsonify({'success': True})

@app.route('/get-movie-details/<int:movie_id>', methods=['GET'])
def get_movie_details(movie_id):
    query = """
        SELECT title, rating, my_rating, directors, year, cast, path
        FROM movies1
        WHERE id = %s
    """
    movie = execute_query(query, (movie_id,))
    return jsonify(movie[0])


@app.route('/update-movie-details/<int:movie_id>', methods=['POST'])
def update_movie_details(movie_id):
    data = request.json
    current_ist_time = get_current_ist_time()

    # Initialize components of the dynamic update query
    set_clauses = []
    params = []

    # Define mapping from input fields to their corresponding database columns
    fields_to_columns = {
        'title': "title = %s",
        'rating': "rating = %s",
        'my_rating': "my_rating = %s",
        'directors': "directors = %s",
        'year': "year = %s",
        'cast': "cast = %s"
    }

    # Populate the SET clauses and parameters list, skip if value is inappropriate
    for field, clause in fields_to_columns.items():
        if field in data:
            # Check for empty strings in numeric fields
            if (field in ['rating', 'my_rating', 'year'] and not data[field].strip()):
                continue  # Skip adding this field to the query if it's empty
            set_clauses.append(clause)
            params.append(data[field])

    # Check if there are any fields to update, to avoid empty SET in SQL
    if not set_clauses:
        return 'No valid fields provided for update', 400

    # Always update the last_modified time
    set_clauses.append("last_modified = %s")
    params.append(current_ist_time)

    # Add the ID to the parameters
    params.append(movie_id)

    # Finalize the query
    update_query = "UPDATE movies1 SET " + ", ".join(set_clauses) + " WHERE id = %s"
    
    # Execute the query
    execute_query(update_query, tuple(params))
    return '', 204

# Route to rename movie file
@app.route('/rename-movie-file/<int:movie_id>', methods=['POST'])
def rename_movie_file(movie_id):
    new_basename = request.json['newBasename']
    query = "SELECT path FROM movies1 WHERE id = %s"
    result = execute_query(query, (movie_id,))
    current_ist_time = get_current_ist_time()
    if result:
        old_path = result[0]['path']
        new_path = os.path.join(os.path.dirname(old_path), new_basename)
        os.rename(old_path, new_path)
        update_query = "UPDATE movies1 SET path = %s,last_modified=%s WHERE id = %s"
        execute_query(update_query, (new_path, current_ist_time,movie_id))
        return '', 204
    return jsonify({}), 404

# Route to delete movie entry
@app.route('/delete-movie/<int:movie_id>', methods=['POST'])
def delete_movie(movie_id):
    query = "DELETE FROM movies1 WHERE id = %s"
    execute_query(query, (movie_id,))
    return '', 204

def get_db_connection(database_name=None):
    config = db_config.copy()
    if database_name:
        config['database'] = database_name
    return mysql.connector.connect(**config)

@app.route('/move-files', methods=['POST'])
def move_files():
    data = request.get_json()
    selected_files = data.get('files', [])
    destination_folder = data.get('destination')
    
    original_database=db_config['database']
    print("Original database:",original_database)
    target_database = data.get('targetDatabase', 'original')
    print("Target database:",target_database)
    current_ist_time = get_current_ist_time()
    
    with get_db_connection(original_database) as db:  # Use the original database
        print("Original Database:", db.database)
        
        for file_id in selected_files:
            result = execute_query("SELECT * FROM movies1 WHERE id = %s", (int(file_id),), connection=db)
            if result:
                result = result[0]
                source_path = result['path']
                file_name = os.path.basename(source_path)
                # Correct path joining to ensure backslashes are properly handled
                if '\\' not in destination_folder:
                    dest_path = os.path.join(destination_folder, '\\'+file_name)
                else:
                    dest_path = os.path.join(destination_folder, file_name)
                print("Source path:", source_path)
                print("Destination path:", dest_path)
                
                try:
                    shutil.move(source_path, dest_path)
                    # Prepare the path for database insertion (handle escaping)
                    dest_path_db = dest_path.replace('\\\\', '\\')
                    
                    if target_database == 'original':
                        print("Database retained")
                        execute_query("UPDATE movies1 SET path = %s, last_modified = %s WHERE id = %s", 
                                       (dest_path_db, current_ist_time, int(file_id)), connection=db)
                        db.commit()  # Ensure changes are committed
                    else:
                        execute_query("delete from movies1 where id = %s", (int(file_id),), connection=db)
                        db.commit()
                        with get_db_connection(target_database) as target_db:
                            print("Database switched to:", target_db.database)
                            cursor = target_db.cursor()
                            cursor.execute('''
                                INSERT INTO movies1 (title, year, my_rating, rating, directors, cast, genres, runtime, languages, release_date, path, last_modified)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ''', (result['title'], result['year'], result['my_rating'], result['rating'], result['directors'], result['cast'], result['genres'],
                                  result['runtime'], result['languages'], result['release_date'], dest_path_db, current_ist_time))
                            target_db.commit()
                        with get_db_connection(original_database) as revert_db:
                            print("Reverted to original database:", revert_db.database)
                except Exception as e:
                    print(f"Failed to move {source_path} to {dest_path}: {e}")
                    return jsonify({'error': str(e)}), 500
    
    return jsonify({"success": True})




@app.route('/copy-files', methods=['POST'])
def copy_files():
    data = request.get_json()
    selected_files = data.get('files', [])
    destination_folder = data.get('destination')
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor()
    for file_id in selected_files:
        cursor.execute("SELECT path FROM movies1 WHERE id = %s", (int(file_id),))
        result = cursor.fetchone()
        if result:
            source_path = result[0]
            file_name = os.path.basename(source_path)
            dest_path = os.path.join(destination_folder, file_name)
            try:
                shutil.copy(source_path, dest_path)
            except Exception as e:
                print(f"Failed to copy {source_path} to {dest_path}: {e}")
    
    return jsonify({"success": True})


@app.route('/backup-database', methods=['POST'])
def backup_database():
    # Check if 'my_rating' column exists
    check_column_query = """
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'backup' AND column_name = 'my_rating'
    """
    column_exists = execute_query(check_column_query)[
        0]['COUNT(*)'] > 0
    flag=0
    # If 'my_rating' column does not exist, add it next to 'rating' column
    if not column_exists:
        flag=1
        add_column_query = """
        ALTER TABLE backup
        ADD COLUMN my_rating FLOAT AFTER rating
        """
        execute_query(add_column_query, fetch=False)
    #print(db_config)
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor()
    try:
        if flag:
            cursor.execute("drop table backup")
        cursor.execute("CREATE TABLE IF NOT EXISTS backup LIKE movies1")
        cursor.execute("TRUNCATE TABLE backup")
        cursor.execute("INSERT INTO backup SELECT * FROM movies1")
        db.commit()
        return jsonify({"message": "Backup complete"}), 200
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        db.close()

@app.route('/recover-database', methods=['POST'])
def recover_database():
    #print(db_config)
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor()
    try:
        cursor.execute("SHOW TABLES LIKE 'backup'")
        result = cursor.fetchone()
        if not result:
            return jsonify({"error": "No backup found"}), 404
        
        cursor.execute("TRUNCATE TABLE movies1")
        cursor.execute("INSERT INTO movies1 SELECT * FROM backup")
        db.commit()
        return jsonify({"message": "Recovery complete"}), 200
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        db.close()

# --- Ensure To See table exists ---
def create_to_see_table_if_not_exists():
    query = "CREATE TABLE IF NOT EXISTS to_see LIKE movies1"
    execute_query(query, fetch=False)

# --- New endpoint to check if a movie is in the To See table ---
@app.route('/check-to-see/<int:movie_id>', methods=['GET'])
def check_to_see(movie_id):
    create_to_see_table_if_not_exists()
    result = execute_query("SELECT * FROM to_see WHERE id = %s", (movie_id,))
    exists = len(result) > 0
    return jsonify({"exists": exists})

# --- Modified Add To See endpoint ---
@app.route('/add-to-see', methods=['POST'])
def add_to_see():
    create_to_see_table_if_not_exists()
    data = request.get_json()
    movie_id = data.get('movie_id')
    if movie_id is None:
        return jsonify({"error": "No movie id provided"}), 400

    # Check if movie already exists in to_see table
    exists = execute_query("SELECT * FROM to_see WHERE id = %s", (movie_id,))
    if exists:
        return jsonify({"error": "Movie already in To See"}), 400

    # Fetch the movie from movies1
    movie_rows = execute_query("SELECT * FROM movies1 WHERE id = %s", (movie_id,))
    if movie_rows:
        movie = movie_rows[0]
        current_ist_time = get_current_ist_time()
        insert_query = """
            INSERT INTO to_see (id, title, year, rating, my_rating, directors, cast, genres, runtime, languages, release_date, path, last_modified)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_query(insert_query, (
            movie['id'], movie['title'], movie['year'], movie['rating'], movie['my_rating'],
            movie['directors'], movie['cast'], movie['genres'], movie['runtime'],
            movie['languages'], movie['release_date'], movie['path'], current_ist_time
        ), fetch=False)
        return jsonify({"success": True})
    else:
        return jsonify({"error": "Movie not found in movies1"}), 404

# --- Modified Remove To See endpoint ---
@app.route('/remove-to-see', methods=['POST'])
def remove_to_see():
    create_to_see_table_if_not_exists()
    data = request.get_json()
    movie_id = data.get('movie_id')
    if movie_id is None:
        return jsonify({"error": "No movie id provided"}), 400
    execute_query("DELETE FROM to_see WHERE id = %s", (movie_id,), fetch=False)
    return jsonify({"success": True})

# --- Endpoint to show all movies in the To See table ---
@app.route('/get-to-see-movies', methods=['GET'])
def get_to_see_movies():
    create_to_see_table_if_not_exists()
    movies = execute_query("SELECT * FROM to_see")
    total_count = len(movies)
    total_runtime = 0
    for movie in movies:
        # Attempt to convert runtime to an integer; if conversion fails, assume 0
        try:
            rt = int(movie.get('runtime', 0))
        except (ValueError, TypeError):
            rt = 0
        total_runtime += rt
    # For the main table view, page runtime equals total runtime since all entries are shown
    return jsonify({
        "movies": movies,
        "total_count": total_count,
        "total_runtime": total_runtime,
        "page_runtime": total_runtime
    })

'''
def run_flask():
    app.run(debug=False)

if __name__ == '__main__':
    # Run Flask app in a separate thread
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    # Create webview window and start the event loop
    webview.create_window("Movie Database", "http://127.0.0.1:5000/")
    webview.start()
'''
if __name__ == "__main__":
    app.run(debug=True)
