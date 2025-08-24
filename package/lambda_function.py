import os
import sys
from uuid import uuid4
from datetime import datetime
import bcrypt
import psycopg2
from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequest, NotFound

app = Flask(__name__)

# ========== Database Connection ==========
def get_connection():
    conn = psycopg2.connect(
        host="saii-database-1.c232u2w86p9f.us-east-1.rds.amazonaws.com",
        database="saii_test",
        user="saiiadmin",
        password="Laksana2010",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute('SET search_path TO nestio;')
    cur.close()
    return conn

# ========== Utility Functions ==========
def serialize_row(row, cur):
    colnames = [desc[0] for desc in cur.description]
    return {
        col: (val.tobytes().decode() if isinstance(val, memoryview) else val)
        for col, val in zip(colnames, row)
    }

def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password, hashed):
    if not hashed:
        return False
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

# ========== User CRUD ==========
def create_user(data):
    conn = get_connection()
    cur = conn.cursor()
    data['user_id'] = str(uuid4())
    data['created_at'] = datetime.now()
    data['password_hash'] = hash_password(data['password'])
    data['role'] = data.get('role', 'user')
    data['is_active'] = True

    query = """
        INSERT INTO "user" (
            user_id, email_id, password_hash, full_name,
            phone_number, role, created_at, is_active
        ) VALUES (
            %(user_id)s, %(email_id)s, %(password_hash)s, %(full_name)s,
            %(phone_number)s, %(role)s, %(created_at)s, %(is_active)s
        )
    """
    try:
        cur.execute(query, data)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise ValueError("User creation failed: " + str(e))
    finally:
        cur.close()
        conn.close()
    return data['user_id']

def get_all_users():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM "user" WHERE is_active = TRUE')
    rows = cur.fetchall()
    result = [serialize_row(row, cur) for row in rows]
    cur.close()
    conn.close()
    return result

def get_user_by_id(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM "user" WHERE user_id = %s', (str(user_id),))
    row = cur.fetchone()
    result = serialize_row(row, cur) if row else None
    cur.close()
    conn.close()
    return result

def update_user(user_id, data):
    conn = get_connection()
    cur = conn.cursor()
    if 'password' in data:
        data['password_hash'] = hash_password(data['password'])
        del data['password']
    fields = ['email_id', 'full_name', 'phone_number', 'password_hash', 'role', 'is_active']
    set_clause = ", ".join([f"{field} = %({field})s" for field in fields if field in data])
    if not set_clause:
        return False
    data['user_id'] = str(user_id)
    query = f'UPDATE "user" SET {set_clause} WHERE user_id = %(user_id)s'
    try:
        cur.execute(query, data)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise ValueError("Update failed: " + str(e))
    finally:
        cur.close()
        conn.close()
    return True

def delete_user(user_id):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute('UPDATE "user" SET is_active = FALSE WHERE user_id = %s', (str(user_id),))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise ValueError("Delete failed: " + str(e))
    finally:
        cur.close()
        conn.close()
    return True

def get_user_by_email_or_phone(identifier):
    conn = get_connection()
    cur = conn.cursor()
    query = """
        SELECT * FROM "user"
        WHERE (email_id = %s OR phone_number = %s) AND is_active = TRUE
    """
    cur.execute(query, (identifier, identifier))
    row = cur.fetchone()
    result = serialize_row(row, cur) if row else None
    cur.close()
    conn.close()
    return result

# ========== Property CRUD ==========
def create_property(data):
    conn = get_connection()
    cur = conn.cursor()
    data['property_id'] = str(uuid4())
    data['created_time'] = datetime.now()
    data['updated_time'] = datetime.now()
    query = """
        INSERT INTO property (
            property_id, developer_name, project_name, project_status,
            property_main_title, project_address, project_area, project_city,
            project_country, price, no_of_beds, no_of_bathrooms, sq_feet,
            property_main_image, latitude, longitude, contact_number, contact_email,
            created_by, created_time, updated_by, updated_time, is_active, pincode, property_type
        ) VALUES (
            %(property_id)s, %(developer_name)s, %(project_name)s, %(project_status)s,
            %(property_main_title)s, %(project_address)s, %(project_area)s, %(project_city)s,
            %(project_country)s, %(price)s, %(no_of_beds)s, %(no_of_bathrooms)s, %(sq_feet)s,
            %(property_main_image)s, %(latitude)s, %(longitude)s, %(contact_number)s, %(contact_email)s,
            %(created_by)s, %(created_time)s, %(updated_by)s, %(updated_time)s, %(is_active)s, %(pincode)s, %(property_type)s
        )
    """
    cur.execute(query, data)
    conn.commit()
    cur.close()
    conn.close()
    return data['property_id']

def get_all_properties():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM property")
    rows = cur.fetchall()
    result = [serialize_row(row, cur) for row in rows]
    cur.close()
    conn.close()
    return result

def get_property_by_id(property_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM property WHERE property_id = %s", (str(property_id),))
    row = cur.fetchone()
    result = serialize_row(row, cur) if row else None
    cur.close()
    conn.close()
    return result

def update_property(property_id, data):
    conn = get_connection()
    cur = conn.cursor()
    data['updated_time'] = datetime.now()
    data['property_id'] = str(property_id)
    fields = [
        'developer_name', 'project_name', 'project_status',
        'property_main_title', 'project_address', 'project_area',
        'project_city', 'project_country', 'price', 'no_of_beds',
        'no_of_bathrooms', 'sq_feet', 'property_main_image',
        'latitude', 'longitude', 'contact_number', 'contact_email',
        'created_by', 'created_time', 'updated_by', 'updated_time',
        'is_active', 'pincode', 'property_type'
    ]
    set_clause = ", ".join([f"{field} = %({field})s" for field in fields])
    query = f"UPDATE property SET {set_clause} WHERE property_id = %(property_id)s"
    cur.execute(query, data)
    conn.commit()
    cur.close()
    conn.close()

def delete_property(property_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM property WHERE property_id = %s", (str(property_id),))
    conn.commit()
    cur.close()
    conn.close()

# ========== API Routes ==========
@app.route('/')
def home():
    return " User and Property APIs Running"

# User Routes
@app.route("/users", methods=["POST"])
def api_create_user():
    data = request.get_json()
    try:
        user_id = create_user(data)
        return jsonify({"message": "User created", "user_id": user_id}), 201
    except ValueError as e:
        raise BadRequest(str(e))

@app.route("/users/<user_id>", methods=["GET"])
def api_get_user(user_id):
    user = get_user_by_id(user_id)
    if not user:
        raise NotFound("User not found")
    return jsonify(user)

@app.route("/users", methods=["GET"])
def api_get_users():
    users = get_all_users()
    return jsonify(users)

@app.route("/user/<user_id>", methods=["PUT"])
def api_update_user(user_id):
    data = request.get_json()
    try:
        update_user(user_id, data)
        return jsonify({"message": "User updated"})
    except ValueError as e:
        raise BadRequest(str(e))

@app.route("/user/<user_id>", methods=["DELETE"])
def api_delete_user(user_id):
    try:
        delete_user(user_id)
        return jsonify({"message": "User deactivated"})
    except ValueError as e:
        raise BadRequest(str(e))

@app.route("/login", methods=["POST"])
def api_login():
    data = request.json
    identifier = data.get("identifier")
    password = data.get("password")
    if not identifier or not password:
        return jsonify({"error": "Email/Phone and password are required"}), 400
    user = get_user_by_email_or_phone(identifier)
    if not user:
        return jsonify({"error": "User not found"}), 404
    if not verify_password(password, user['password_hash']):
        return jsonify({"error": "Invalid password"}), 401
    return jsonify({"message": "Login successful", "user_id": user['user_id']})

# Property Routes
@app.route('/properties', methods=['GET'])
def get_all():
    return jsonify(get_all_properties())

@app.route('/properties/<property_id>', methods=['GET'])
def get_one(property_id):
    result = get_property_by_id(property_id)
    return jsonify(result) if result else ("Not found", 404)

@app.route('/properties', methods=['POST'])
def create():
    data = request.json
    property_id = create_property(data)
    return jsonify({"property_id": property_id})

@app.route('/properties/<property_id>', methods=['PUT'])
def update(property_id):
    data = request.json
    update_property(property_id, data)
    return jsonify({"message": "Property updated"})

@app.route('/properties/<property_id>', methods=['DELETE'])
def delete(property_id):
    delete_property(property_id)
    return jsonify({"message": "Property deleted"})

# For AWS Lambda

def lambda_handler(event, context):
    import awsgi
    return awsgi.response(app, event, context)

# For local testing
if __name__ == '__main__':
    app.run(debug=True)
