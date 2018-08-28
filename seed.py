"""Utility file to seed ratings database from MovieLens data in seed_data/"""

from sqlalchemy import func
from model import User
from model import Rating
from model import Movie

from model import connect_to_db, db
from server import app
import datetime


def load_users(users_file):
    """Load users from u.user into database."""

    print("Users")

    # Delete all rows in table, so if we need to run this a second time,
    # we won't be trying to add duplicate users
    User.query.delete()

    # Read u.user file and insert data
    for i, row in enumerate(open(users_file)):
        row = row.rstrip()
        user_id, age, gender, occupation, zipcode = row.split("|")

        users = User(user_id=user_id,
                    age=age,
                    zipcode=zipcode)

        # We need to add to the session or it won't ever be stored
        db.session.add(users)

    # Once we're done, we should commit our work
    db.session.commit()


def load_movies(movies_file):
    """Load movies from u.item into database."""
    print("Movies")

    # Delete all rows in table, so if we need to run this a second time,
    # we won't be trying to add duplicate users
    Movie.query.delete()

    # Read u.movie file and insert data
    for i, row in enumerate(open(movies_file)):
        row = row.rstrip()
        row = row.split("|")
        movie_id = row[0]
        title = row[1][:-7]
        released_str = row[2]
        video_release_date = row[3]
        imdb_url = row[4]
        # movie_id, title, released_str, imdb_url, timestamp = row.split("|")[:5]

        # date_time_funct(released_str) 
        if released_str:
            released_at = datetime.datetime.strptime(released_str, "%d-%b-%Y")
        else:
            released_at = None

        # released_at = date_time_funct(released_str)

        movies = Movie(movie_id=movie_id,
                    title=title,
                    released_at=released_at,
                    imdb_url = imdb_url)

        # We need to add to the session or it won't ever be stored
        db.session.add(movies)

    # Once we're done, we should commit our work
    db.session.commit()


def load_ratings(ratings_file):
    """Load ratings from u.data into database."""
    print("Ratings")

    # Delete all rows in table, so if we need to run this a second time,
    # we won't be trying to add duplicate users
    Rating.query.delete()

    # Read u.user file and insert data
    for row in open(ratings_file):
        row = row.rstrip()
        row = row.split("\t")
        user_id = row[0]
        movie_id = row[1]
        score = row[2] 
        timestamp = row[3]


        rating = Rating(movie_id=movie_id,
                    user_id=user_id,
                    score=score)

      
      # score=score  # We need to add to the session or it won't ever be stored
        db.session.add(rating)

    # Once we're done, we should commit our work
    db.session.commit()


def set_val_user_id():
    """Set value for the next user_id after seeding database"""

    # Get the Max user_id in the database
    result = db.session.query(func.max(User.user_id)).one()
    max_id = int(result[0])

    # Set the value for the next user_id to be max_id + 1
    query = "SELECT setval('users_user_id_seq', :new_id)"
    db.session.execute(query, {'new_id': max_id + 1})
    db.session.commit()


if __name__ == "__main__":
    connect_to_db(app)

    # In case tables haven't been created, create them
    db.create_all()

    # Import different types of data
    users_file =  "seed_data/u.user"
    movies_file = "seed_data/u.item"
    ratings_file = "seed_data/u.data"
    load_users(users_file)?
    load_movies(movies_file)
    load_ratings(ratings_file)
    set_val_user_id()
