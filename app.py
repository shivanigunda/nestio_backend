import os
import sys
from uuid import uuid4
from functools import wraps
import bcrypt
import psycopg2
import uuid
import hashlib
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




# ========== Property CRUD ==========
def create_property(data):
    conn = get_connection()
    cur = conn.cursor()
    data['property_id'] = str(uuid4())
    query = """
        INSERT INTO property (
            property_id, property_title, property_type, purpose,
            completion_status, total_price, project_name, street,
            area, city_town, state, digicode, country,
            sq_feet, beds, bathrooms, contact_number, contact_email,
            additional_discription, facing, brokerage_involved, latitude, longitude, amenities, approval_status, main_image, additional_images_videos
        ) VALUES (
            %(property_id)s, %(property_title)s, %(property_type)s, %(purpose)s,
            %(completion_status)s, %(total_price)s, %(project_name)s, %(street)s,
            %(area)s, %(city_town)s, %(state)s, %(digicode)s, %(country)s,
            %(sq_feet)s, %(beds)s, %(bathrooms)s, %(contact_number)s, %(contact_email)s,
            %(additional_discription)s, %(facing)s, %(brokerage_involved)s, %(latitude)s, %(longitude)s, %(amenities)s, %(approval_status)s, %(main_image)s, %(additional_images_videos)s
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
    data['property_id'] = str(property_id)
    fields = [
            'property_title', 'property_type', 'purpose',
            'completion_status', 'total_price', 'project_name', 'street',
            'area', 'city_town', 'state', 'digicode', 'country',
            'sq_feet', 'beds', 'bathrooms', 'contact_number', 'contact_email',
            'additional_discription', 'facing', 'brokerage_involved', 'latitude', 'longitude', 'amenities', 'approval_status', 'main_image', 'additional_images_videos'
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

def sign_up(full_name, phone_number, email_id, password):
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    conn = get_connection()
    cur = conn.cursor()
    user_id = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO sign_up (user_id, email_id, password_hash, full_name, phone_number)
        VALUES (%s, %s, %s, %s, %s)
    """, (user_id, email_id, password_hash, full_name, phone_number))
    conn.commit()
    cur.close()
    conn.close()

    return user_id


# ========== API Routes ==========
@app.route('/')
def home():
    return "Combined User and Property API Running"


# Property Routes
@app.route('/nestio/v1/properties', methods=['GET'])
def get_all():
    return jsonify(get_all_properties())

@app.route('/nestio/v1/properties/<property_id>', methods=['GET'])
def get_one(property_id):
    result = get_property_by_id(property_id)
    return jsonify(result) if result else ("Not found", 404)

@app.route('/nestio/v1/properties', methods=['POST'])
def create():
    data = request.json
    property_id = create_property(data)
    return jsonify({"property_id": property_id})


@app.route('/nestio/v1/properties/<property_id>', methods=['PUT'])
def update(property_id):
    data = request.json
    update_property(property_id, data)
    return jsonify({"message": "Property updated"})

@app.route('/nestio/v1/properties/<property_id>', methods=['DELETE'])
def delete(property_id):
    delete_property(property_id)
    return jsonify({"message": "Property deleted"})

@app.route('/nestio/v1/user', methods=['POST'])
def signup():
    data = request.get_json()
    print("Incoming JSON:", data)

    full_name = data.get('full_name')
    phone_number = data.get('phone_number')
    email_id = data.get('email_id', '')
    password = data.get('password')

    if not full_name or not email_id or not phone_number or not password:
        return jsonify({"error": "Missing fields"}), 400

    user_id = sign_up(full_name, phone_number, email_id, password)
    return jsonify({"message": "Signup successful", "user_id": user_id}), 201



# For local testing
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)