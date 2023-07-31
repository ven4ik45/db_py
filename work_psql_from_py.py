import psycopg2


def create_db_name():
    """создание БД, если она не существует"""
    conn = psycopg2.connect(dbname='postgres', user='postgre', password='postgre', host='127.0.0.1')
    cursor = conn.cursor()
    conn.autocommit = True
    cursor.execute("SELECT COUNT(*) = 0 FROM pg_catalog.pg_database WHERE datname = 'clients'")
    not_exists, = cursor.fetchone()
    if not_exists:
        cursor.execute("""CREATE DATABASE clients""")
    cursor.close()
    conn.close()


def create_db_tables(conn):
    """Создаем таблицы в БД"""
    cur = conn.cursor()
    query_create_students = """create TABLE if not exists clients(
                            id SERIAL primary key,
                            name VARCHAR(40) not null,
                            surname VARCHAR(60) not null,
                            email VARCHAR(80) unique not null
                            );"""
    query_create_phones = """create TABLE if not exists phones (
                            id SERIAL primary key,
                            phone VARCHAR(11) not null,
                            client_id integer not null references clients(id) on delete cascade
                            );"""
    cur.execute(query_create_students)
    cur.execute(query_create_phones)
    conn.commit()


def add_new_client(conn, name, surname, email, phone=None):
    """Функция, позволяющая добавить нового клиента"""
    query = """insert into clients (name, surname, email)
            values (%s, %s, %s)
            RETURNING id;"""
    cur = conn.cursor()
    cur.execute(query, (name, surname, email))
    id_client = cur.fetchall()
    if phone:
        phone = phone.replace('+', '')
        query_for_add_phone = """insert into phones (phone, client_id)
                values (%s, %s);"""
        cur.execute(query_for_add_phone, (phone, id_client[0][0]))
    conn.commit()


def add_phone_for_client(conn, phone, email):
    """Функция, позволяющая добавить телефон для существующего клиента
    для выбора нужного нам клиента, будем использовать его email"""
    phone = phone.replace('+', '')
    query = """insert into phones (phone, client_id)
            values (%s, (select id from clients c
            where email = %s));"""
    cur = conn.cursor()
    cur.execute(query, (phone, email))
    conn.commit()


def update_client(conn, email, name=None, surname=None, new_email=None, phone=None, new_phone=None):
    """Функция, позволяющая изменить данные о клиенте
    для выбора нужного нам клиента, будем использовать его email"""
    query_upd = []
    params = []
    cur = conn.cursor()
    cur.execute("select id from clients where email=%s", (email,))
    client_id = cur.fetchall()
    if name:
        query_upd.append("name=%s")
        params.append(name)
    if surname:
        query_upd.append("surname=%s")
        params.append(surname)
    if new_email:
        query_upd.append("email=%s")
        params.append(new_email)
    if name or surname or new_email:
        query_update_client = "update clients set " + ', '.join(query_upd) + " where email=%s;"
        cur.execute(query_update_client, tuple(params + [email]))

    query_update_phone = """update phones 
                        set phone = %s
                        where phone = %s and client_id = %s;"""
    if phone and new_phone and client_id:
        phone = phone.replace('+', '')
        new_phone = new_phone.replace('+', '')
        cur.execute(query_update_phone, (new_phone, phone, client_id[0][0]))
    if (phone and not new_phone) or (new_phone and not phone):
        print('Необходимо указать текущий номер телефона, и номер телефона, на который нужно заменить!!!')
    conn.commit()


def del_phone(conn, phone, email):
    """Функция, позволяющая удалить телефон для существующего клиента"""
    phone = phone.replace('+', '')
    cur = conn.cursor()
    client_id = cur.execute("""select id from clients
                            where email = %s;""", (email,))
    query = """delete from phones
            where phone = %s and client_id = %s;"""
    cur.execute(query, (phone, client_id))
    conn.commit()


def del_client(conn, email):
    """Функция, позволяющая удалить существующего клиента"""
    query = """delete from clients 
            where email = %s;"""
    cur = conn.cursor()
    cur.execute(query, (email,))
    conn.commit()


def find_client(conn, name='%', surname='%', email='%', phone=None):
    """Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону"""
    cur = conn.cursor()
    if phone:
        query = """select c.*, p.phone from clients c
                        left join phones p on c.id = p.client_id
                        where c.name like %s and c.surname like %s and c.email like %s and phone like %s"""
        cur.execute(query, ('%' + name + '%', '%' + surname + '%', '%' + email + '%', phone))
        answer = cur.fetchall()
        for i in answer:
            print(f'id клиента: {i[0]}, имя: {i[1]}, фамилия: {i[2]}, email: {i[3]}, телефон: {i[4]}')
    else:
        query = """select c.*, p.phone from clients c
                left join phones p on c.id = p.client_id
                where c.name like %s and c.surname like %s and c.email like %s"""
        cur.execute(query, ('%' + name + '%', '%' + surname + '%', '%' + email + '%'))
        answer = cur.fetchall()
        for i in answer:
            print(f'id клиента: {i[0]}, имя: {i[1]}, фамилия: {i[2]}, email: {i[3]}, телефон: {i[4]}')


# def find_client(conn, name='%', surname=None, email=None, phone=None):
#     """Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону"""
#     cur = conn.cursor()
#     if name:
#         query = """select c.*, p.phone from clients c
#                 left join phones p on c.id = p.client_id
#                 where c.name like %s"""
#         cur.execute(query, ('%' + name + '%',))
#         answer = cur.fetchall()
#         for i in answer:
#             print(f'id клиента: {i[0]}, имя: {i[1]}, фамилия: {i[2]}, email: {i[3]}, телефон: {i[4]}')
#     if surname:
#         query = """select c.*, p.phone from clients c
#                 left join phones p on c.id = p.client_id
#                 where c.surname like %s"""
#         cur.execute(query, ('%' + surname + '%',))
#         answer = cur.fetchall()
#         for i in answer:
#             print(f'id клиента: {i[0]}, имя: {i[1]}, фамилия: {i[2]}, email: {i[3]}, телефон: {i[4]}')
#     if email:
#         query = """select c.*, p.phone from clients c
#                 left join phones p on c.id = p.client_id
#                 where c.email = %s"""
#         cur.execute(query, (email,))
#         answer = cur.fetchall()
#         for i in answer:
#             print(f'id клиента: {i[0]}, имя: {i[1]}, фамилия: {i[2]}, email: {i[3]}, телефон: {i[4]}')
#     if phone:
#         phone = phone.replace('+', '')
#         query = """select c.*, p.phone from clients c
#                 left join phones p on c.id = p.client_id
#                 where p.phone = %s"""
#         cur.execute(query, (phone,))
#         answer = cur.fetchall()
#         for i in answer:
#             print(f'id клиента: {i[0]}, имя: {i[1]}, фамилия: {i[2]}, email: {i[3]}, телефон: {i[4]}')


def show_all_clients(conn):
    """Функция отображающая весь список клиентов и их телефонов"""
    cur = conn.cursor()
    query = """select c.*, p.phone from clients c
            left join phones p on c.id = p.client_id;"""
    cur.execute(query)
    answer = cur.fetchall()
    for i in answer:
        print(f'id клиента: {i[0]}, имя: {i[1]}, фамилия: {i[2]}, email: {i[3]}, телефон: {i[4]}')


def show_all_clients_without_phone(conn):
    """Функция отображающая список клиентов у которых нет телефонов"""
    cur = conn.cursor()
    query = """select c.*, p.phone from clients c
            left join phones p on c.id = p.client_id
            where p.phone is null;"""
    cur.execute(query)
    answer = cur.fetchall()
    for i in answer:
        print(f'id клиента: {i[0]}, имя: {i[1]}, фамилия: {i[2]}, email: {i[3]}, телефон: {i[4]}')


create_db_name()
with psycopg2.connect(dbname="clients", user="postgre", password="postgre", host="127.0.0.1") as conn:
    create_db_tables(conn)
    add_new_client(conn, name='Dmitry', surname='Kozlov', email='dmkoz@gmail.com', phone='+79998881122')
    add_new_client(conn, name='Dmitry2', surname='Kozlov2', email='dmkoz2@gmail.com', phone='+79998881123')
    add_new_client(conn, name='Alex', surname='Dubov', email='adubov@gmail.com', phone='89998882233')
    add_new_client(conn, name='Elena', surname='Kacheva', email='kachelena@mail.ru')
    add_new_client(conn, name='Anna', surname='Ivanova', email='ianna@yahoo.com', phone='89998883344')
    add_new_client(conn, name='Irina', surname='Korina', email='ikori@mail.ru')

    add_phone_for_client(conn, phone='+79998885566', email='ikori@mail.ru')
    add_phone_for_client(conn, phone='+79998880011', email='adubov@gmail.com')
    add_phone_for_client(conn, phone='+79997773344', email='ianna@yahoo.com')

    update_client(conn, email='adubov@gmail.com', name='Alexey', phone='79998880011', new_phone='89998882222')

    del_phone(conn, phone='+79997773344', email='ianna@yahoo.com')

    del_client(conn, email='dmkoz2@gmail.com')

    find_client(conn, name='Dmitry')
    find_client(conn, surname='Dubov')
    find_client(conn, email='ikori@mail.ru')
    find_client(conn, phone='89998882222')

    show_all_clients(conn)

    show_all_clients_without_phone(conn)


conn.close()
