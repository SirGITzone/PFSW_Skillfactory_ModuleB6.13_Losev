import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


DB_PATH = "sqlite:///albums.sqlite3"
Base = declarative_base()

class Incorrect(Exception):
    pass


class AlreadyExists(Incorrect):
    pass


class Album(Base):
    """
    Описание структуры таблицы album для хранения записей музыкальной библиотеки
    """

    __tablename__ = "album"

    id = sa.Column(sa.INTEGER, primary_key=True)
    artist = sa.Column(sa.TEXT)
    genre = sa.Column(sa.TEXT)
    album = sa.Column(sa.TEXT)


def connect_db():
    """
    Установление соединения с базой данных, создание таблицы, если таковая отсуствует. Возвращает объект сессии
    """
    engine = sa.create_engine(DB_PATH)
    Base.metadata.create_all(engine)
    session = sessionmaker(engine)
    return session()


def find(artist):
    """
    Находит все альбомы в базе данных по заданному артисту
    """
    session = connect_db()
    albums = session.query(Album).filter(Album.artist == artist).all()
    return albums


def new(artist, genre, album):
    assert isinstance(artist, str), "Incorrect artist"
    assert isinstance(genre, str), "Incorrect genre"
    assert isinstance(album, str), "Incorrect album"

    session = connect_db()
    saved_album = session.query(Album).filter(Album.album == album, Album.artist == artist).first()
    if saved_album is not None:
        raise AlreadyExists("Album already exists and has #{}".format(saved_album.id))

    album = Album(artist=artist, genre=genre, album=album)

    session.add(album)
    session.commit()
    return album
