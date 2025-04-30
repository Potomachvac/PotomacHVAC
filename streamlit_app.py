import streamlit as st
import snowflake.snowpark as sp
from snowflake.snowpark.functions import col
cnx=st.connection("snowflake")
session = cnx.session()
from snowflake.snowpark import Session
from datetime import datetime, timedelta
import re
import uuid
import hashlib
import base64
from PIL import Image
import io
# Add this helper function with your imports
from PIL import Image, ImageOps
import io

def crop_to_square(image_bytes):
    img = Image.open(io.BytesIO(image_bytes))
    return ImageOps.fit(img, (120, 120))  # Crop to square

def process_image(image_bytes, target_width):
    """Resize image while maintaining quality and aspect ratio"""
    img = Image.open(io.BytesIO(image_bytes))
    
    # Calculate proportional height
    width_percent = (target_width / float(img.size[0]))
    height = int((float(img.size[1]) * float(width_percent)))
    
    # High-quality resizing
    img = img.resize((target_width, height), Image.Resampling.LANCZOS)
    
    # Convert back to bytes
    buffer = io.BytesIO()
    img.save(buffer, format="JPEG", quality=95)  # 95% quality
    return buffer.getvalue()

##########################################################################################
##########################################################################################


# Initialize Snowflake connection
def get_session():
    return sp.context.get_active_session()

# Role-based access control
ROLE_ACCESS = {
    'admin': ['home', 'profile', 'customers', 'appointments', 'quotes', 'jobs', 'invoices', 'payments', 'reports', 'analytics', 'admin_tables', 'equipment'],
    'office': ['home', 'customers', 'appointments', 'equipment'],
    'technician': ['home', 'profile', 'quotes', 'jobs', 'invoices', 'payments', 'equipment'],
    'driver': ['home', 'profile', 'driver_tasks']
}
##########################################################################################
##########################################################################################
# Login page
def login_page():
    st.title("POTOMAC HVAC")
    emp_id = st.text_input("Employee ID")
    password = st.text_input("Password", type='password')
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Login"):
            session = get_session()
            try:
                result = session.sql(f"""
                    SELECT e.*, r.rolename
                    FROM employees e
                    JOIN employee_roles er ON e.employeeid = er.employeeid
                    JOIN roles r ON er.roleid = r.roleid
                    WHERE e.employeeid = '{emp_id}' AND e.password = '{password}'
                """).collect()
                if result:
                    st.session_state.update({
                        'logged_in': True,
                        'user_id': emp_id,
                        'user_name': result[0]['ENAME'],
                        'roles': [row['ROLENAME'] for row in result]
                    })
                    st.rerun()
                else:
                    st.error("Invalid credentials")
            except Exception as e:
                st.error(f"Login error: {str(e)}")
    with col2:
        if st.button("Forgot Password?"):
            st.session_state['show_forgot_password'] = True

    # Show "Forgot Password" section if enabled
    if st.session_state.get('show_forgot_password'):
        forgot_password()

        # Add a "Back to Login" button only in the "Forgot Password" flow
        if st.button("Back to Login"):
            st.session_state['show_forgot_password'] = False
            st.rerun()
##########################################################################################
# Forgot password functionality
def forgot_password():
    st.subheader("🔒 Forgot Password")
    email = st.text_input("Enter your email address")
    if st.button("Send Reset Link"):
        session = get_session()
        try:
            employee = session.sql(f"""
                SELECT employeeid FROM employees
                WHERE email = '{email}'
            """).collect()
            if employee:
                employee_id = employee[0]['EMPLOYEEID']
                reset_token = str(uuid.uuid4())
                token_hash = hashlib.sha256(reset_token.encode()).hexdigest()
                expires_at = datetime.now() + timedelta(hours=1)
                session.sql(f"""
                    INSERT INTO password_resets
                    (resetid, employeeid, reset_token, expires_at)
                    VALUES (
                        '{str(uuid.uuid4())}',
                        '{employee_id}',
                        '{token_hash}',
                        '{expires_at}'
                    )
                """).collect()
                st.success("Password reset link sent to your email!")
            else:
                st.error("No account found with that email address")
        except Exception as e:
            st.error(f"Error processing request: {str(e)}")
##########################################################################################
# Reset password functionality
def reset_password(token):
    st.subheader("🔑 Reset Password")
    new_password = st.text_input("New Password", type='password')
    confirm_password = st.text_input("Confirm Password", type='password')
    if st.button("Reset Password"):
        if new_password == confirm_password:
            session = get_session()
            try:
                token_hash = hashlib.sha256(token.encode()).hexdigest()
                reset_record = session.sql(f"""
                    SELECT * FROM password_resets
                    WHERE reset_token = '{token_hash}'
                    AND used = FALSE
                    AND expires_at > CURRENT_TIMESTAMP()
                """).collect()
                if reset_record:
                    employee_id = reset_record[0]['EMPLOYEEID']
                    session.sql(f"""
                        UPDATE employees
                        SET password = '{new_password}'
                        WHERE employeeid = '{employee_id}'
                    """).collect()
                    session.sql(f"""
                        UPDATE password_resets
                        SET used = TRUE
                        WHERE resetid = '{reset_record[0]['RESETID']}'
                    """).collect()
                    st.success("Password reset successfully!")
                    st.session_state.clear()
                    st.rerun()
                else:
                    st.error("Invalid or expired reset token")
            except Exception as e:
                st.error(f"Error resetting password: {str(e)}")
        else:
            st.error("Passwords do not match")
##########################################################################################
# home
def home():
    session = get_session()
    
    # Welcome section with picture
    col_welcome, col_picture = st.columns([4, 1])
    with col_welcome:
        st.subheader("Home")
        st.write(f"Welcome, **{st.session_state.user_name}**!")
        st.write(f"**Role:** {', '.join(st.session_state.roles)}")
    
    with col_picture:    
        picture_info = session.sql(f"""
            SELECT PICTURE_DATA_TEXT FROM EMPLOYEE_PICTURES
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
            ORDER BY UPLOADED_AT DESC
            LIMIT 1
        """).collect()
        
        if picture_info and picture_info[0]['PICTURE_DATA_TEXT']:
            try:
                image_data = base64.b64decode(picture_info[0]['PICTURE_DATA_TEXT'])
                processed_img = process_image(image_data, 120)
                st.image(processed_img, width=120, caption="Your profile")
            except Exception as e:
                st.error(f"Couldn't load picture: {str(e)}")
        else:
            st.info("No picture")

    # Date selection for time entries
    selected_date = st.date_input("Select Date", datetime.now().date())
    
    # Manual time entry option
    manual_time = st.checkbox("Enter times manually", key="manual_time_entry")
    
    # Initialize time variables
    clock_in_time = None
    clock_out_time = None
    break_in_time = None
    break_end_time = None

    # Time entry fields (shown only if manual_time is True)
    if manual_time:
        col1, col2 = st.columns(2)
        with col1:
            clock_in_time = st.time_input("Clock In Time", key="manual_clock_in")
        with col2:
            clock_out_time = st.time_input("Clock Out Time", key="manual_clock_out", 
                                         disabled=(not clock_in_time))
        
        col3, col4 = st.columns(2)
        with col3:
            break_in_time = st.time_input("Break Start Time", key="manual_break_in",
                                        disabled=(not clock_in_time))
        with col4:
            break_end_time = st.time_input("Break End Time", key="manual_break_end",
                                         disabled=(not break_in_time))

    # Fetch time and break entries for selected date
    time_entry = session.sql(f"""
        SELECT * FROM employee_time_entries
        WHERE EMPLOYEEID = '{st.session_state.user_id}'
        AND ENTRY_DATE = '{selected_date}'
        ORDER BY CLOCK_IN DESC
        LIMIT 1
    """).collect()

    break_entry = session.sql(f"""
        SELECT * FROM employee_break_entries
        WHERE EMPLOYEEID = '{st.session_state.user_id}'
        AND ENTRY_DATE = '{selected_date}'
        ORDER BY BREAK_START DESC
        LIMIT 1
    """).collect()

    # Determine current state
    has_time_entry = time_entry and len(time_entry) > 0
    has_break_entry = break_entry and len(break_entry) > 0
    
    is_clocked_in = has_time_entry and not time_entry[0]['CLOCK_OUT']
    is_on_break = has_break_entry and not break_entry[0]['BREAK_END']

    # Time entry buttons with state management
    st.subheader("Time Tracking")
    
    if manual_time:
        if st.button("Save Manual Time Entry"):
            # Validate time sequence
            error = False
            
            # Convert to datetime objects for comparison
            clock_in_dt = datetime.combine(selected_date, clock_in_time) if clock_in_time else None
            clock_out_dt = datetime.combine(selected_date, clock_out_time) if clock_out_time else None
            break_in_dt = datetime.combine(selected_date, break_in_time) if break_in_time else None
            break_end_dt = datetime.combine(selected_date, break_end_time) if break_end_time else None
            
            if clock_out_time and clock_in_time and clock_out_time <= clock_in_time:
                st.error("Clock out time must be after clock in time")
                error = True
            
            if break_in_time:
                if not clock_in_time:
                    st.error("Break can only be recorded after clocking in")
                    error = True
                elif break_in_time <= clock_in_time:
                    st.error("Break start must be after clock in time")
                    error = True
            
            if break_end_time:
                if not break_in_time:
                    st.error("Break end can only be recorded after break start")
                    error = True
                elif break_end_time <= break_in_time:
                    st.error("Break end must be after break start")
                    error = True
            
            if clock_out_time and break_end_time and clock_out_time <= break_end_time:
                st.error("Clock out must be after break end")
                error = True
            
            if not error:
                # Handle manual time entry
                entry_id = f"ENTRY{datetime.now().timestamp()}"
                session.sql(f"""
                    INSERT INTO employee_time_entries
                    (ENTRYID, EMPLOYEEID, CLOCK_IN, CLOCK_OUT, ENTRY_DATE)
                    VALUES (
                        '{entry_id}',
                        '{st.session_state.user_id}',
                        '{clock_in_dt}'{(', NULL' if not clock_out_time else f", '{clock_out_dt}'")},
                        '{selected_date}'
                    )
                """).collect()
                
                if break_in_time:
                    break_id = f"BREAK{datetime.now().timestamp()}"
                    session.sql(f"""
                        INSERT INTO employee_break_entries
                        (BREAKID, EMPLOYEEID, BREAK_START, BREAK_END, ENTRY_DATE)
                        VALUES (
                            '{break_id}',
                            '{st.session_state.user_id}',
                            '{break_in_dt}'{(', NULL' if not break_end_time else f", '{break_end_dt}'")},
                            '{selected_date}'
                        )
                    """).collect()
                
                st.success(f"Time entries saved for {selected_date}!")
                st.rerun()
    else:
        col1, col2 = st.columns(2)
        with col1:
            # Clock In button - only enabled if not currently clocked in for selected date
            if st.button("Clock In", 
                        key=f"clock_in_{selected_date}",
                        disabled=is_clocked_in,
                        help="Cannot clock in again if already clocked in for this date"):
                entry_id = f"ENTRY{datetime.now().timestamp()}"
                session.sql(f"""
                    INSERT INTO employee_time_entries
                    (ENTRYID, EMPLOYEEID, CLOCK_IN, ENTRY_DATE)
                    VALUES (
                        '{entry_id}',
                        '{st.session_state.user_id}',
                        CURRENT_TIMESTAMP(),
                        '{selected_date}'
                    )
                """).collect()
                st.success(f"Clocked in successfully for {selected_date}!")
                st.rerun()
        
        with col2:
            # Clock Out button - only enabled if clocked in and not on break
            if st.button("Clock Out", 
                        key=f"clock_out_{selected_date}",
                        disabled=not is_clocked_in or is_on_break,
                        help="Cannot clock out while on break or if not clocked in"):
                if has_time_entry:
                    session.sql(f"""
                        UPDATE employee_time_entries
                        SET CLOCK_OUT = CURRENT_TIMESTAMP()
                        WHERE ENTRYID = '{time_entry[0]['ENTRYID']}'
                    """).collect()
                    st.success(f"Clocked out successfully for {selected_date}!")
                    st.rerun()

        # Break management buttons
        col3, col4 = st.columns(2)
        with col3:
            # Break In button - only enabled if clocked in and not already on break
            if st.button("Break In", 
                        key=f"break_in_{selected_date}",
                        disabled=not is_clocked_in or is_on_break,
                        help="Cannot start break if not clocked in or already on break"):
                break_id = f"BREAK{datetime.now().timestamp()}"
                session.sql(f"""
                    INSERT INTO employee_break_entries
                    (BREAKID, EMPLOYEEID, BREAK_START, ENTRY_DATE)
                    VALUES (
                        '{break_id}',
                        '{st.session_state.user_id}',
                        CURRENT_TIMESTAMP(),
                        '{selected_date}'
                    )
                """).collect()
                st.success(f"Break started successfully for {selected_date}!")
                st.rerun()
        
        with col4:
            # Break Out button - only enabled if currently on break
            if st.button("Break Out", 
                        key=f"break_out_{selected_date}",
                        disabled=not is_on_break,
                        help="Cannot end break if not currently on break"):
                if has_break_entry:
                    session.sql(f"""
                        UPDATE employee_break_entries
                        SET BREAK_END = CURRENT_TIMESTAMP()
                        WHERE BREAKID = '{break_entry[0]['BREAKID']}'
                    """).collect()
                    st.success(f"Break ended successfully for {selected_date}!")
                    st.rerun()

    # Current status display
    st.subheader("Current Status")
    status_col1, status_col2 = st.columns(2)
    with status_col1:
        # Clock status
        if is_clocked_in and has_time_entry:
            st.success("✅ Currently Clocked In")
            st.write(f"**Clock In Time:** {time_entry[0]['CLOCK_IN']}")
        elif has_time_entry and time_entry[0]['CLOCK_OUT']:
            st.info("🕒 Clocked Out")
            st.write(f"**Clock In Time:** {time_entry[0]['CLOCK_IN']}")
            st.write(f"**Clock Out Time:** {time_entry[0]['CLOCK_OUT']}")
        else:
            st.warning("⏱ Not Clocked In")
        
        # Break status
        if is_on_break and has_break_entry:
            st.error("⏸ Currently On Break")
            st.write(f"**Break Start Time:** {break_entry[0]['BREAK_START']}")
        elif has_break_entry and break_entry[0]['BREAK_END']:
            st.info("☑️ Break Completed")
            st.write(f"**Break Time:** {break_entry[0]['BREAK_START']} to {break_entry[0]['BREAK_END']}")
        elif is_clocked_in:
            st.success("✅ Available for Break")
        else:
            st.info("ℹ️ Break not available (not clocked in)")
    
    with status_col2:
        # Calculate and display hours
        if has_time_entry:
            if time_entry[0]['CLOCK_OUT']:
                clock_in = time_entry[0]['CLOCK_IN']
                clock_out = time_entry[0]['CLOCK_OUT']
                total_hours = (clock_out - clock_in).total_seconds() / 3600
                
                if has_break_entry and break_entry[0]['BREAK_END']:
                    break_start = break_entry[0]['BREAK_START']
                    break_end = break_entry[0]['BREAK_END']
                    break_hours = (break_end - break_start).total_seconds() / 3600
                    net_hours = total_hours - break_hours
                    
                    st.metric("Total Hours", f"{total_hours:.2f}")
                    st.metric("Break Time", f"{break_hours:.2f}")
                    st.metric("Net Hours", f"{net_hours:.2f}")
                else:
                    st.metric("Total Hours", f"{total_hours:.2f}")
            else:
                st.info("⏳ Currently working - clock out to see total hours")
        else:
            st.info("No time entries for selected date")

    # Rest of your existing code for appointments and work summary...
    # [Keep the existing appointments and work summary sections from your original code]

    

              
        

    
   
    # Appointments section
    with st.container(border=True):
        st.subheader("📅 Your Appointments")
        
        # Get all appointments (today + upcoming 7 days)
        appointments = session.sql(f"""
            SELECT 
                a.appointmentid,
                c.name AS customer_name,
                c.phone AS customer_phone,
                c.address AS customer_address,
                a.scheduled_time,
                a.status AS appointment_status,
                CASE 
                    WHEN DATE(a.scheduled_time) = CURRENT_DATE() THEN 'Today'
                    WHEN DATE(a.scheduled_time) = DATEADD('day', 1, CURRENT_DATE()) THEN 'Tomorrow'
                    ELSE TO_VARCHAR(a.scheduled_time, 'Mon DD')
                END AS display_date
            FROM appointments a
            JOIN customers c ON a.customerid = c.customerid
            WHERE a.technicianid = '{st.session_state.user_id}'
            AND DATE(a.scheduled_time) BETWEEN CURRENT_DATE() AND DATEADD('day', 7, CURRENT_DATE())
            ORDER BY a.scheduled_time
        """).collect()
        
        if not appointments:
            st.info("No appointments scheduled for the next 7 days")
        else:
            # Group appointments by date
            appointments_by_date = {}
            status_counts = {'Pending': 0, 'Completed': 0}
            
            for appt in appointments:
                status = appt['APPOINTMENT_STATUS']
                if status == 'completed':
                    status_counts['Completed'] += 1
                else:
                    status_counts['Pending'] += 1
                
                date_key = appt['DISPLAY_DATE']
                if date_key not in appointments_by_date:
                    appointments_by_date[date_key] = []
                appointments_by_date[date_key].append(appt)
            
            # Display summary metrics
            cols = st.columns(3)
            cols[0].metric("Total", len(appointments))
            cols[1].metric("Pending", status_counts['Pending'])
            cols[2].metric("Completed", status_counts['Completed'])
            
            st.divider()
            
            # Display appointments grouped by date
            for date_key, date_appointments in appointments_by_date.items():
                with st.expander(f"{date_key} ({len(date_appointments)} appointments)"):
                    for appt in date_appointments:
                        col1, col2, col3 = st.columns([0.1, 3, 1])
                        
                        with col1:
                            is_completed = st.checkbox(
                                "",
                                value=appt['APPOINTMENT_STATUS'] == 'completed',
                                key=f"complete_{appt['APPOINTMENTID']}",
                                label_visibility="collapsed"
                            )
                            if is_completed != (appt['APPOINTMENT_STATUS'] == 'completed'):
                                new_status = 'completed' if is_completed else 'pending'
                                session.sql(f"""
                                    UPDATE appointments
                                    SET status = '{new_status}'
                                    WHERE appointmentid = '{appt['APPOINTMENTID']}'
                                """).collect()
                                st.rerun()
                        
                        with col2:
                            st.markdown(f"""
                                **{appt['CUSTOMER_NAME']}**  
                                <small>{appt['SCHEDULED_TIME'].strftime('%I:%M %p')} • {appt['CUSTOMER_ADDRESS']}</small>
                            """, unsafe_allow_html=True)
                        
                        with col3:
                            status_badge = {
                                'pending': ("🟠 Pending", "orange"),
                                'completed': ("🟢 Completed", "green"),
                                'canceled': ("🔴 Canceled", "red"),
                                'rescheduled': ("🔵 Rescheduled", "blue")
                            }.get(appt['APPOINTMENT_STATUS'].lower(), ("⚪ Unknown", "gray"))
                            
                            st.markdown(
                                f"<span style='color:{status_badge[1]};font-weight:bold'>{status_badge[0]}</span>",
                                unsafe_allow_html=True
                            )
                    
                    st.divider()

    # Work Summary Section
    with st.container(border=True):
        st.subheader("Work Summary")
        
        # Calculate hours worked for selected date
        hours_result = session.sql(f"""
            SELECT SUM(TIMEDIFF('MINUTE', CLOCK_IN, CLOCK_OUT)/60.0) AS total_hours
            FROM employee_time_entries
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
            AND ENTRY_DATE = '{selected_date}'
            AND CLOCK_OUT IS NOT NULL
        """).collect()
        
        hours_worked = float(hours_result[0]['TOTAL_HOURS']) if hours_result and hours_result[0]['TOTAL_HOURS'] is not None else 0.0
        
        # Get hourly rate
        hourly_rate_result = session.sql(f"""
            SELECT HOURLYRATE FROM employees 
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
        """).collect()
        hourly_rate = float(hourly_rate_result[0]['HOURLYRATE']) if hourly_rate_result else 0.0
        
        # Calculate completed appointments for selected date
        completed_today = session.sql(f"""
            SELECT COUNT(*) AS completed
            FROM appointments
            WHERE technicianid = '{st.session_state.user_id}'
            AND DATE(scheduled_time) = '{selected_date}'
            AND status = 'completed'
        """).collect()[0]['COMPLETED']
        
        # Display summary metrics
        summary_cols = st.columns(3)
        with summary_cols[0]:
            st.metric("Hours Worked", f"{hours_worked:.2f}")
        with summary_cols[1]:
            st.metric("Jobs Completed", completed_today)
        with summary_cols[2]:
            earnings = hours_worked * hourly_rate
            st.metric("Earnings", f"${earnings:.2f}")
    



        





#######################################################################

#Profile
def profile_page():
    session = get_session()
    st.title(f"👤 {st.session_state.user_name}'s Profile")
    
    # ============ PROFILE PICTURE SECTION ============
    col1, col2 = st.columns([1, 2])
    with col1:
        # Fetch and display profile picture
        picture = session.sql(f"""
            SELECT PICTURE_DATA_TEXT FROM EMPLOYEE_PICTURES
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
            ORDER BY UPLOADED_AT DESC LIMIT 1
        """).collect()
        
        if picture and picture[0]['PICTURE_DATA_TEXT']:
            try:
                image_data = base64.b64decode(picture[0]['PICTURE_DATA_TEXT'])
                st.image(process_image(image_data, 200), width=200)
            except Exception as e:
                st.error(f"Error loading image: {str(e)}")
        else:
            st.image(Image.new('RGB', (200, 200), color='gray'), width=200)
            
        # Profile picture uploader
        uploaded_file = st.file_uploader("Update Profile Picture", type=["jpg", "jpeg", "png"])
        if uploaded_file:
            try:
                img = Image.open(uploaded_file)
                if img.mode == 'RGBA':
                    img = img.convert('RGB')
                img.thumbnail((800, 800), Image.Resampling.LANCZOS)
                buffer = io.BytesIO()
                img.save(buffer, format="JPEG", quality=90)
                encoded_image = base64.b64encode(buffer.getvalue()).decode('utf-8')
                
                session.sql(f"""
                    INSERT INTO EMPLOYEE_PICTURES 
                    (PICTUREID, EMPLOYEEID, PICTURE_DATA_TEXT)
                    VALUES (
                        'PIC{datetime.now().timestamp()}',
                        '{st.session_state.user_id}',
                        '{encoded_image}'
                    )
                """).collect()
                st.success("Profile picture updated!")
                st.rerun()
            except Exception as e:
                st.error(f"Upload failed: {str(e)}")
    
    with col2:
        # ============ EMPLOYEE STATS SUMMARY ============
        st.subheader("Performance Summary")
        
        # Get hourly rate
        hourly_rate = float(session.sql(f"""
            SELECT HOURLYRATE FROM employees 
            WHERE EMPLOYEEID = '{st.session_state.user_id}'
        """).collect()[0]['HOURLYRATE'])
        
        # Time period selector
        time_period = st.selectbox("View Period", 
                                 ["Today", "This Week", "This Month", "This Year", "Custom Range"],
                                 key="profile_period")
        
        # Date range handling
        start_date, end_date = None, None
        if time_period == "Custom Range":
            col_a, col_b = st.columns(2)
            with col_a:
                start_date = st.date_input("Start Date")
            with col_b:
                end_date = st.date_input("End Date")
        
        # Build date filter SQL with table prefixes
        date_filter = ""
        if time_period == "Today":
            date_filter = "AND e.ENTRY_DATE = CURRENT_DATE()"
        elif time_period == "This Week":
            date_filter = "AND e.ENTRY_DATE >= DATE_TRUNC('WEEK', CURRENT_DATE())"
        elif time_period == "This Month":
            date_filter = "AND e.ENTRY_DATE >= DATE_TRUNC('MONTH', CURRENT_DATE())"
        elif time_period == "This Year":
            date_filter = "AND e.ENTRY_DATE >= DATE_TRUNC('YEAR', CURRENT_DATE())"
        elif time_period == "Custom Range" and start_date and end_date:
            date_filter = f"AND e.ENTRY_DATE BETWEEN '{start_date}' AND '{end_date}'"
        
        # ============ WORK STATISTICS ============
        # Get appointment stats
        appointments = session.sql(f"""
            SELECT 
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending
            FROM appointments
            WHERE technicianid = '{st.session_state.user_id}'
            {date_filter.replace('e.ENTRY_DATE', 'DATE(scheduled_time)')}
        """).collect()[0]
        
        # Get time tracking stats with proper table prefixes
        time_stats = session.sql(f"""
            SELECT 
                SUM(TIMEDIFF('MINUTE', e.CLOCK_IN, e.CLOCK_OUT)/60.0) AS total_hours,
                SUM(TIMEDIFF('MINUTE', b.BREAK_START, b.BREAK_END)/60.0) AS total_breaks
            FROM employee_time_entries e
            LEFT JOIN employee_break_entries b 
                ON e.EMPLOYEEID = b.EMPLOYEEID 
                AND e.ENTRY_DATE = b.ENTRY_DATE
            WHERE e.EMPLOYEEID = '{st.session_state.user_id}'
            {date_filter}
            AND e.CLOCK_OUT IS NOT NULL
        """).collect()[0]
        
        # Display summary metrics
        metric_cols = st.columns(4)
        metric_cols[0].metric("Appointments", appointments['TOTAL'])
        metric_cols[1].metric("Completed", appointments['COMPLETED'])
        metric_cols[2].metric("Hours Worked", f"{float(time_stats['TOTAL_HOURS'] or 0):.1f}")
        metric_cols[3].metric("Earnings", f"${(float(time_stats['TOTAL_HOURS'] or 0) * hourly_rate):.2f}")
    
    # ============ DETAILED TIME TRACKING TABLE ============

    st.subheader("Detailed Time Records")
    
    # Date search functionality
    search_col, _ = st.columns([1, 3])
    with search_col:
        search_date = st.date_input("Search by Date", key="date_search")
    
    # Get detailed time entries
    time_data = session.sql(f"""
        SELECT 
            e.ENTRY_DATE,
            e.CLOCK_IN,
            e.CLOCK_OUT,
            b.BREAK_START,
            b.BREAK_END,
            TIMEDIFF('MINUTE', e.CLOCK_IN, e.CLOCK_OUT)/60.0 AS work_hours,
            TIMEDIFF('MINUTE', b.BREAK_START, b.BREAK_END)/60.0 AS break_hours,
            (work_hours - COALESCE(break_hours, 0)) * {hourly_rate} AS daily_earnings
        FROM employee_time_entries e
        LEFT JOIN employee_break_entries b 
            ON e.EMPLOYEEID = b.EMPLOYEEID 
            AND e.ENTRY_DATE = b.ENTRY_DATE
        WHERE e.EMPLOYEEID = '{st.session_state.user_id}'
        {date_filter}
        ORDER BY e.ENTRY_DATE DESC
    """).collect()
    
    if time_data:
        # Convert to pandas DataFrame with proper datetime conversion
        import pandas as pd
        df = pd.DataFrame([{
            "Date": pd.to_datetime(row['ENTRY_DATE']),  # Ensure datetime conversion
            "Clock In": row['CLOCK_IN'],
            "Clock Out": row['CLOCK_OUT'],
            "Break Start": row['BREAK_START'] if row['BREAK_START'] else "N/A",
            "Break End": row['BREAK_END'] if row['BREAK_END'] else "N/A",
            "Work Hours": float(row['WORK_HOURS']) if row['WORK_HOURS'] else 0.0,
            "Break Hours": float(row['BREAK_HOURS']) if row['BREAK_HOURS'] else 0.0,
            "Daily Earnings": float(row['DAILY_EARNINGS']) if row['DAILY_EARNINGS'] else 0.0
        } for row in time_data])
        
        # Apply date search filter if specified
        if search_date:
            # Convert search_date to datetime for comparison
            search_dt = pd.to_datetime(search_date)
            df = df[df['Date'].dt.date == search_dt.date()]
        
        # Calculate totals
        total_hours = df['Work Hours'].sum()
        total_breaks = df['Break Hours'].sum()
        total_earnings = df['Daily Earnings'].sum()
        
        # Format the display
        display_df = df.copy()
        display_df["Date"] = display_df["Date"].dt.strftime('%Y-%m-%d')  # Format date for display
        display_df["Work Hours"] = display_df["Work Hours"].apply(lambda x: f"{x:.2f}")
        display_df["Break Hours"] = display_df["Break Hours"].apply(lambda x: f"{x:.2f}")
        display_df["Daily Earnings"] = display_df["Daily Earnings"].apply(lambda x: f"${x:.2f}")
        
        # Add totals row at the bottom
        totals_row = pd.DataFrame([{
            "Date": "TOTAL",
            "Clock In": "",
            "Clock Out": "",
            "Break Start": "",
            "Break End": "",
            "Work Hours": f"{total_hours:.2f}",
            "Break Hours": f"{total_breaks:.2f}",
            "Daily Earnings": f"${total_earnings:.2f}"
        }])
        
        display_df = pd.concat([display_df, totals_row], ignore_index=True)
        
        # Display the dataframe with styled totals row
        st.dataframe(
            display_df.style.apply(
                lambda x: ['font-weight: bold' if x.name == len(display_df)-1 else '' for i in x], 
                axis=1
            ),
            use_container_width=True,
            height=min(400, 35 * (len(display_df) + 38)
        ))
        
        # Show totals below table as well
        st.markdown(f"""
            **Period Totals:**  
            - **Total Hours Worked:** {total_hours:.2f} hours  
            - **Total Break Time:** {total_breaks:.2f} hours  
            - **Total Earnings:** ${total_earnings:.2f}
        """)
    else:
        st.info("No time records found for the selected period.")

            
#######################################################################
def equipment_management():
    st.subheader("🛠️ Equipment Management")
    session = get_session()

    # Search and select a customer
    st.subheader("Select Customer")
    search_query = st.text_input("Search by Name or Phone")

    # Fetch customers based on search query
    if search_query:
        customers = session.sql(f"""
            SELECT c.*, cu.* 
            FROM customers c
            LEFT JOIN customer_units cu ON c.customerid = cu.customerid
            WHERE c.NAME ILIKE '%{search_query}%' OR c.PHONE ILIKE '%{search_query}%'
        """).collect()
    else:
        customers = session.sql("""
            SELECT c.*, cu.* 
            FROM customers c
            LEFT JOIN customer_units cu ON c.customerid = cu.customerid
        """).collect()

    if not customers:
        st.warning("No customers found.")
        return

    customer_options = {row['CUSTOMERID']: f"{row['NAME']} ({row['PHONE']})" for row in customers}
    selected_customer_id = st.selectbox(
        "Select Customer",
        options=customer_options.keys(),
        format_func=lambda x: customer_options[x]
    )

    if not selected_customer_id:
        return

    # Get selected customer details
    selected_customer = next((c for c in customers if c['CUSTOMERID'] == selected_customer_id), None)
    
    if not selected_customer:
        st.error("Customer not found")
        return

    # Display customer info header
    st.write(f"### Equipment for {selected_customer['NAME']}")
    st.write(f"**Address:** {selected_customer['ADDRESS']}")
    st.write(f"**Phone:** {selected_customer['PHONE']}")

    # Check if equipment info exists
    if not selected_customer['UNITID']:
        st.warning("No equipment information recorded for this customer.")
        
        # Add new equipment form
        with st.expander("➕ Add New Equipment Record"):
            with st.form("add_equipment_form"):
                st.subheader("Outdoor Unit")
                outdoor_unit = st.text_input("Outdoor Unit Model")
                outdoor_unit_age = st.number_input("Outdoor Unit Age (years)", min_value=0, max_value=50, step=1)
                
                st.subheader("Indoor Unit")
                indoor_unit = st.text_input("Indoor Unit Model")
                indoor_unit_age = st.number_input("Indoor Unit Age (years)", min_value=0, max_value=50, step=1)
                
                st.subheader("Thermostat")
                thermostat_type = st.text_input("Thermostat Type")
                thermostat_age = st.number_input("Thermostat Age (years)", min_value=0, max_value=50, step=1)
                
                st.subheader("Location & Accessibility")
                unit_location = st.selectbox(
                    "Unit Location",
                    ["Attic", "Basement", "Garage", "Closet", "Crawlspace", "Other"]
                )
                accessibility_level = st.selectbox(
                    "Accessibility Level",
                    ["Easy", "Moderate", "Difficult", "Very Difficult"]
                )
                accessibility_notes = st.text_area("Accessibility Notes")
                other_notes = st.text_area("Other Notes")
                
                if st.form_submit_button("Save Equipment Record"):
                    unit_id = f"UNIT{datetime.now().timestamp()}"
                    session.sql(f"""
                        INSERT INTO customer_units 
                        (UNITID, CUSTOMERID, OUTDOOR_UNIT, OUTDOOR_UNIT_AGE, 
                         INDOOR_UNIT, INDOOR_UNIT_AGE, THERMOSTAT_TYPE, THERMOSTAT_AGE,
                         UNIT_LOCATION, ACCESSIBILITY_LEVEL, ACCESSIBILITY_NOTES, OTHER_NOTES)
                        VALUES (
                            '{unit_id}',
                            '{selected_customer_id}',
                            '{outdoor_unit}',
                            {outdoor_unit_age},
                            '{indoor_unit}',
                            {indoor_unit_age},
                            '{thermostat_type}',
                            {thermostat_age},
                            '{unit_location}',
                            '{accessibility_level}',
                            '{accessibility_notes}',
                            '{other_notes}'
                        )
                    """).collect()
                    st.success("Equipment record added successfully!")
                    st.rerun()
    else:
        # Display existing equipment info
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Outdoor Unit")
            st.write(f"**Model:** {selected_customer['OUTDOOR_UNIT'] or 'Not recorded'}")
            st.write(f"**Age:** {selected_customer['OUTDOOR_UNIT_AGE'] or 'N/A'} years")
            
            st.subheader("Indoor Unit")
            st.write(f"**Model:** {selected_customer['INDOOR_UNIT'] or 'Not recorded'}")
            st.write(f"**Age:** {selected_customer['INDOOR_UNIT_AGE'] or 'N/A'} years")
            
        with col2:
            st.subheader("Thermostat")
            st.write(f"**Type:** {selected_customer['THERMOSTAT_TYPE'] or 'Not recorded'}")
            st.write(f"**Age:** {selected_customer['THERMOSTAT_AGE'] or 'N/A'} years")
            
            st.subheader("Location")
            st.write(f"**Location:** {selected_customer['UNIT_LOCATION'] or 'Not recorded'}")
            st.write(f"**Accessibility:** {selected_customer['ACCESSIBILITY_LEVEL'] or 'Not recorded'}")
        
        st.subheader("Additional Notes")
        st.write(f"**Accessibility Notes:**")
        st.info(selected_customer['ACCESSIBILITY_NOTES'] or "No notes available")
        st.write(f"**Other Notes:**")
        st.info(selected_customer['OTHER_NOTES'] or "No notes available")

        # Edit equipment info
        with st.expander("✏️ Edit Equipment Information"):
            with st.form("edit_equipment_form"):
                st.subheader("Outdoor Unit")
                outdoor_unit = st.text_input("Outdoor Unit Model", value=selected_customer['OUTDOOR_UNIT'] or "")
                outdoor_unit_age = st.number_input("Outdoor Unit Age (years)", 
                                                 min_value=0, max_value=50, step=1,
                                                 value=selected_customer['OUTDOOR_UNIT_AGE'] or 0)
                
                st.subheader("Indoor Unit")
                indoor_unit = st.text_input("Indoor Unit Model", value=selected_customer['INDOOR_UNIT'] or "")
                indoor_unit_age = st.number_input("Indoor Unit Age (years)", 
                                                min_value=0, max_value=50, step=1,
                                                value=selected_customer['INDOOR_UNIT_AGE'] or 0)
                
                st.subheader("Thermostat")
                thermostat_type = st.text_input("Thermostat Type", value=selected_customer['THERMOSTAT_TYPE'] or "")
                thermostat_age = st.number_input("Thermostat Age (years)", 
                                              min_value=0, max_value=50, step=1,
                                              value=selected_customer['THERMOSTAT_AGE'] or 0)
                
                st.subheader("Location & Accessibility")
                unit_location = st.selectbox(
                    "Unit Location",
                    ["Attic", "Basement", "Garage", "Closet", "Crawlspace", "Other"],
                    index=["Attic", "Basement", "Garage", "Closet", "Crawlspace", "Other"].index(
                        selected_customer['UNIT_LOCATION'] or "Attic")
                )
                accessibility_level = st.selectbox(
                    "Accessibility Level",
                    ["Easy", "Moderate", "Difficult", "Very Difficult"],
                    index=["Easy", "Moderate", "Difficult", "Very Difficult"].index(
                        selected_customer['ACCESSIBILITY_LEVEL'] or "Easy")
                )
                accessibility_notes = st.text_area("Accessibility Notes", 
                                                 value=selected_customer['ACCESSIBILITY_NOTES'] or "")
                other_notes = st.text_area("Other Notes", 
                                         value=selected_customer['OTHER_NOTES'] or "")
                
                if st.form_submit_button("Update Equipment Record"):
                    session.sql(f"""
                        UPDATE customer_units 
                        SET OUTDOOR_UNIT = '{outdoor_unit}',
                            OUTDOOR_UNIT_AGE = {outdoor_unit_age},
                            INDOOR_UNIT = '{indoor_unit}',
                            INDOOR_UNIT_AGE = {indoor_unit_age},
                            THERMOSTAT_TYPE = '{thermostat_type}',
                            THERMOSTAT_AGE = {thermostat_age},
                            UNIT_LOCATION = '{unit_location}',
                            ACCESSIBILITY_LEVEL = '{accessibility_level}',
                            ACCESSIBILITY_NOTES = '{accessibility_notes}',
                            OTHER_NOTES = '{other_notes}'
                        WHERE CUSTOMERID = '{selected_customer_id}'
                    """).collect()
                    st.success("Equipment record updated successfully!")
                    st.rerun()


#######################################################################
# Customer management with unit information
def customer_management():
    st.subheader("👥 Customer Management")
    session = get_session()

    # Add a new customer
    with st.expander("Add New Customer"):
        with st.form("add_customer_form"):
            name = st.text_input("Full Name")
            phone = st.text_input("Phone (###-###-####)")
            email = st.text_input("Email")
            address = st.text_input("Address")
            city = st.text_input("City")
            state = st.selectbox("State", ["MD", "DC", "VA"])
            zipcode = st.text_input("Zip Code")
            latitude = st.number_input("Latitude", format="%.6f")
            longitude = st.number_input("Longitude", format="%.6f")
            # Customer Request
            st.subheader("📝 Customer Request")
            request = st.text_area("")

            

           

            if st.form_submit_button("Add Customer"):
                if re.match(r"^\d{3}-\d{3}-\d{4}$", phone):
                    customer_id = f"CUST{datetime.now().timestamp()}"
                    session.sql(f"""
                        INSERT INTO customers 
                        (CUSTOMERID, NAME, PHONE, EMAIL, ADDRESS, LATITUDE, LONGITUDE, REQUEST)
                        VALUES (
                            '{customer_id}',
                            '{name}',
                            '{phone}',
                            '{email}',
                            '{address}, {city}, {state} {zipcode}',
                            {latitude},
                            {longitude},
                            '{request}'
                        )
                    """).collect()

                    

                    
                else:
                    st.error("Invalid phone number format")

    # Search and select a customer
    st.subheader("Select Customer")
    search_query = st.text_input("Search by Name or Phone")

    # Fetch customers based on search query
    if search_query:
        customers = session.sql(f"""
            SELECT * FROM customers 
            WHERE NAME ILIKE '%{search_query}%' OR PHONE ILIKE '%{search_query}%'
        """).collect()
    else:
        customers = session.sql("SELECT * FROM customers").collect()

    customer_options = {row['CUSTOMERID']: f"{row['NAME']} ({row['PHONE']})" for row in customers}
    selected_customer_id = st.selectbox(
        "Select Customer",
        options=customer_options.keys(),
        format_func=lambda x: customer_options[x]
    )

    if selected_customer_id:
        # Fetch customer details
        customer_details = session.sql(f"""
            SELECT * FROM customers 
            WHERE CUSTOMERID = '{selected_customer_id}'
        """).collect()[0]

        # Display customer details
        st.subheader("Customer Details")
        st.write(f"**Name:** {customer_details['NAME']}")
        st.write(f"**Phone:** {customer_details['PHONE']}")
        st.write(f"**Email:** {customer_details['EMAIL']}")
        st.write(f"**Address:** {customer_details['ADDRESS']}")
        st.write(f"**Latitude:** {customer_details['LATITUDE']}")
        st.write(f"**Longitude:** {customer_details['LONGITUDE']}")

        

        # Fetch unit information for the selected customer
        unit_info = session.sql(f"""
            SELECT * FROM customer_units 
            WHERE CUSTOMERID = '{selected_customer_id}'
        """).collect()

        if unit_info:
            for unit in unit_info:
                st.write(f"**Outdoor Unit:** {unit['OUTDOOR_UNIT']}")
                st.write(f"**Outdoor Unit Age:** {unit['OUTDOOR_UNIT_AGE']} years")
                st.write(f"**Indoor Unit:** {unit['INDOOR_UNIT']}")
                st.write(f"**Indoor Unit Age:** {unit['INDOOR_UNIT_AGE']} years")
                st.write(f"**Thermostat Type:** {unit['THERMOSTAT_TYPE']}")
                st.write(f"**Unit Location:** {unit['UNIT_LOCATION']}")
                st.write(f"**Accessibility Notes:** {unit['ACCESSIBILITY_NOTES']}")
                st.write(f"**Other Notes:** {unit['OTHER_NOTES']}")
                st.write("---")
    

      

        # Edit customer details
        with st.expander("Edit Customer Details"):
            with st.form("edit_customer_form"):
                name = st.text_input("Full Name", value=customer_details['NAME'])
                phone = st.text_input("Phone (###-###-####)", value=customer_details['PHONE'])
                email = st.text_input("Email", value=customer_details['EMAIL'])
                address = st.text_input("Address", value=customer_details['ADDRESS'])
                latitude = st.number_input("Latitude", value=float(customer_details['LATITUDE']), format="%.6f")
                longitude = st.number_input("Longitude", value=float(customer_details['LONGITUDE']), format="%.6f")
                request = st.text_area("Customer Request", value=customer_details['REQUEST'])

                if st.form_submit_button("Update Customer"):
                    if re.match(r"^\d{3}-\d{3}-\d{4}$", phone):
                        session.sql(f"""
                            UPDATE customers 
                            SET NAME = '{name}',
                                PHONE = '{phone}',
                                EMAIL = '{email}',
                                ADDRESS = '{address}',
                                LATITUDE = {latitude},
                                LONGITUDE = {longitude},
                                REQUEST = '{request}'
                            WHERE CUSTOMERID = '{selected_customer_id}'
                        """).collect()
                        st.success("Customer updated successfully!")
                        st.rerun()
                    else:
                        st.error("Invalid phone number format")

        # Remove customer
        with st.expander("Remove Customer"):
            st.write(f"Are you sure you want to remove **{customer_details['NAME']}**?")
            if st.button("Remove Customer"):
                session.sql(f"""
                    DELETE FROM customers 
                    WHERE CUSTOMERID = '{selected_customer_id}'
                """).collect()
                st.success("Customer removed successfully!")
                st.rerun()

#######################################################################

# Appointments
def appointments():
    st.subheader("📅 Appointments")
    session = get_session()

    # Fetch customers and technicians
    customers = session.sql("SELECT customerid, name FROM customers").collect()
    customer_options = {row['CUSTOMERID']: row['NAME'] for row in customers}

    technicians = session.sql("SELECT employeeid, ename FROM employees WHERE employeeid IN (SELECT employeeid FROM employee_roles WHERE roleid = 'RL003')").collect()
    tech_options = {row['EMPLOYEEID']: row['ENAME'] for row in technicians}

    with st.form("appointment_form"):
        customer_id = st.selectbox(
            "Select Customer",
            options=customer_options.keys(),
            format_func=lambda x: customer_options[x]
        )
        technician_id = st.selectbox(
            "Select Technician",
            options=tech_options.keys(),
            format_func=lambda x: tech_options[x]
        )
        appointment_date = st.date_input("Appointment Date")
        appointment_time = st.time_input("Appointment Time")

        if st.form_submit_button("Schedule Appointment"):
            appointment_id = f"APT{datetime.now().timestamp()}"
            scheduled_datetime = datetime.combine(appointment_date, appointment_time)
            session.sql(f"""
                INSERT INTO appointments 
                (appointmentid, customerid, technicianid, scheduled_time)
                VALUES (
                    '{appointment_id}',
                    '{customer_id}',
                    '{technician_id}',
                    '{scheduled_datetime}'
                )
            """).collect()
            st.success("Appointment scheduled successfully!")
#######################################################################

# Quotes
# Quotes Tab
def quotes():
    st.subheader("📝 Quotes")
    session = get_session()
    
    # Fetch customers assigned to the logged-in technician
    if 'technician' in [role.lower() for role in st.session_state.roles]:
        customers = session.sql(f"""
            SELECT c.customerid, c.name, c.email 
            FROM appointments a
            JOIN customers c ON a.customerid = c.customerid
            WHERE a.technicianid = '{st.session_state.user_id}'
        """).collect()
    else:
        customers = session.sql("SELECT customerid, name, email FROM customers").collect()
    
    customer_options = {row['CUSTOMERID']: f"{row['NAME']} ({row['EMAIL']})" for row in customers}
    
    # Define equipment, materials, and labor options
    equipment_options = {
        'CARRIER': 'Carrier',
        'BRIAN': 'Brian'
    }
    material_options = {
        'GAS': 'Gas',
        'FILTER': 'Filter'
    }
    labor_options = {
        'LABOR1': 'Labor1',
        'LABOR2': 'Labor2'
    }
    
    # Initialize session state for dynamic form
    if 'quote_items' not in st.session_state:
        st.session_state.quote_items = []

    with st.form("quote_form"):
        # Title and description
        st.write("### Potomac HVAC Quote")
        job_description = st.text_area("Job Description")
        
        # Customer selection
        customer_id = st.selectbox(
            "Select Customer",
            options=customer_options.keys(),
            format_func=lambda x: customer_options[x]
        )
        
        # Date and time
        quote_date = st.date_input("Quote Date (MM/DD/YYYY)")
        quote_time = st.time_input("Quote Time")
        
        # Add equipment, materials, and labor fees dynamically
        st.write("### Add Items to Quote")
        
        # Equipment Section
        st.write("#### Equipment")
        col1, col2, col3 = st.columns(3)
        with col1:
            equipment_id = st.selectbox(
                "Select Equipment",
                options=list(equipment_options.keys()),
                format_func=lambda x: equipment_options[x],
                key="equipment_id"
            )
        with col2:
            equipment_price = st.number_input("Price", min_value=0.0, step=0.01, key="equipment_price")
        with col3:
            if st.form_submit_button("Add Equipment"):
                st.session_state.quote_items.append({
                    'type': 'Equipment',
                    'id': equipment_id,
                    'price': equipment_price
                })
        
        # Materials Section
        st.write("#### Materials")
        col1, col2, col3 = st.columns(3)
        with col1:
            material_id = st.selectbox(
                "Select Material",
                options=list(material_options.keys()),
                format_func=lambda x: material_options[x],
                key="material_id"
            )
        with col2:
            material_price = st.number_input("Price", min_value=0.0, step=0.01, key="material_price")
        with col3:
            if st.form_submit_button("Add Material"):
                st.session_state.quote_items.append({
                    'type': 'Material',
                    'id': material_id,
                    'price': material_price
                })
        
        # Labor Section
        st.write("#### Labor")
        col1, col2, col3 = st.columns(3)
        with col1:
            labor_id = st.selectbox(
                "Select Labor",
                options=list(labor_options.keys()),
                format_func=lambda x: labor_options[x],
                key="labor_id"
            )
        with col2:
            labor_price = st.number_input("Price", min_value=0.0, step=0.01, key="labor_price")
        with col3:
            if st.form_submit_button("Add Labor"):
                st.session_state.quote_items.append({
                    'type': 'Labor',
                    'id': labor_id,
                    'price': labor_price
                })
        
        # Display added items
        st.write("### Items in Quote")
        if st.session_state.quote_items:
            for idx, item in enumerate(st.session_state.quote_items):
                st.write(f"{idx + 1}. {item['type']} - {item['id']} - ${item['price']:.2f}")
        
        # Calculate total
        total = sum(item['price'] for item in st.session_state.quote_items)
        st.write(f"**Total:** ${total:,.2f}")
        
        if st.form_submit_button("Create Quote"):
            quote_id = f"QUOTE{datetime.now().timestamp()}"
            session.sql(f"""
                INSERT INTO quotes 
                (quoteid, customerid, quote_date, quote_time, total, description, employeeid)
                VALUES (
                    '{quote_id}',
                    '{customer_id}',
                    '{quote_date}',
                    '{quote_time}',
                    {total},
                    '{job_description}',
                    '{st.session_state.user_id}'
                )
            """).collect()
            
            # Insert quote items
            for item in st.session_state.quote_items:
                session.sql(f"""
                    INSERT INTO quote_items 
                    (quoteid, item_type, item_id, price)
                    VALUES (
                        '{quote_id}',
                        '{item['type']}',
                        '{item['id']}',
                        {item['price']}
                    )
                """).collect()
            
            st.success("Quote created successfully!")
            st.session_state.quote_items = []  # Clear items after submission


#######################################################################

# Jobs
def jobs():
    st.subheader("🔧 Jobs")
    session = get_session()

    # Fetch quotes for dropdown
    quotes = session.sql("SELECT QUOTEID, CUSTOMERID FROM quotes").collect()
    quote_options = {row['QUOTEID']: f"Quote {row['QUOTEID']} (Customer {row['CUSTOMERID']})" for row in quotes}

    # Fetch technicians for dropdown
    technicians = session.sql("""
        SELECT EMPLOYEEID, ENAME 
        FROM employees 
        WHERE EMPLOYEEID IN (SELECT EMPLOYEEID FROM employee_roles WHERE ROLEID = 'RL003')
    """).collect()
    tech_options = {row['EMPLOYEEID']: row['ENAME'] for row in technicians}

    # Create a new job
    with st.form("job_form"):
        quote_id = st.selectbox(
            "Select Quote",
            options=quote_options.keys(),
            format_func=lambda x: quote_options[x]
        )
        technician_id = st.selectbox(
            "Select Technician",
            options=tech_options.keys(),
            format_func=lambda x: tech_options[x]
        )
        status = st.selectbox("Job Status", ["pending", "in_progress", "completed"])

        if st.form_submit_button("Create Job"):
            job_id = f"JOB{datetime.now().timestamp()}"
            session.sql(f"""
                INSERT INTO jobs 
                (JOBID, QUOTEID, TECHNICIANID, STATUS)
                VALUES (
                    '{job_id}',
                    '{quote_id}',
                    '{technician_id}',
                    '{status}'
                )
            """).collect()
            st.success("Job created successfully!")
            st.rerun()

    # Display all jobs
    st.write("### All Jobs")
    jobs = session.sql("SELECT * FROM jobs").collect()
    if jobs:
        st.dataframe(jobs)
    else:
        st.info("No jobs found.")
##########################################################################################
# Invoices
def invoices():
    st.subheader("🧾 Invoices")
    session = get_session()

    # Fetch jobs
    jobs = session.sql("SELECT jobid, quoteid FROM jobs").collect()
    job_options = {row['JOBID']: row['QUOTEID'] for row in jobs}

    with st.form("invoice_form"):
        job_id = st.selectbox(
            "Select Job",
            options=job_options.keys(),
            format_func=lambda x: job_options[x]
        )
        total_amount = st.number_input("Total Amount", min_value=0.0, step=0.01)
        description = st.text_area("Invoice Description")

        if st.form_submit_button("Create Invoice"):
            invoice_id = f"INV{datetime.now().timestamp()}"
            session.sql(f"""
                INSERT INTO invoices 
                (invoiceid, jobid, total_amount, description)
                VALUES (
                    '{invoice_id}',
                    '{job_id}',
                    {total_amount},
                    '{description}'
                )
            """).collect()
            st.success("Invoice created successfully!")

#######################################################################
# Payments
def payments():
    st.subheader("💳 Payments")
    session = get_session()

    # Fetch invoices
    invoices = session.sql("SELECT invoiceid, jobid FROM invoices").collect()
    invoice_options = {row['INVOICEID']: row['JOBID'] for row in invoices}

    with st.form("payment_form"):
        invoice_id = st.selectbox(
            "Select Invoice",
            options=invoice_options.keys(),
            format_func=lambda x: invoice_options[x]
        )
        amount = st.number_input("Amount", min_value=0.0, step=0.01)
        payment_method = st.selectbox("Payment Method", ["cash", "check", "credit card"])

        if st.form_submit_button("Process Payment"):
            payment_id = f"PAY{datetime.now().timestamp()}"
            session.sql(f"""
                INSERT INTO payments 
                (paymentid, invoiceid, amount, payment_method)
                VALUES (
                    '{payment_id}',
                    '{invoice_id}',
                    {amount},
                    '{payment_method}'
                )
            """).collect()
            st.success("Payment processed successfully!")
#######################################################################

# Reports
def reports():
    st.subheader("📈 Reports")
    session = get_session()

    # Fetch all jobs
    jobs = session.sql("SELECT * FROM jobs").collect()
    if jobs:
        st.write("### Jobs Report")
        st.dataframe(jobs)
    else:
        st.write("No jobs found.")
#######################################################################
# Analytics
# Analytics Page
def analytics():
    st.subheader("📈 Analytics")
    session = get_session()

    # Display analytics
    st.write("### Total Revenue")
    total_revenue = session.sql("SELECT SUM(TOTAL) FROM invoices").collect()[0][0]
    st.write(f"**Total Revenue:** ${total_revenue:,.2f}")

    st.write("### Total Customers")
    total_customers = session.sql("SELECT COUNT(*) FROM customers").collect()[0][0]
    st.write(f"**Total Customers:** {total_customers}")

    st.write("### Total Jobs")
    total_jobs = session.sql("SELECT COUNT(*) FROM jobs").collect()[0][0]
    st.write(f"**Total Jobs:** {total_jobs}")
#######################################################################
# Admin Tables
# Admin Tables
# Admin Tab: Manage All Tables
def admin_tables():
    st.subheader("🛠 Admin Tables")
    session = get_session()
    
    # List of all tables
    tables = [
        "employees", "customers", "appointments", "quotes", "jobs", 
        "invoices", "roles", "employee_roles", "payment_methods", 
        "payments", "allservices", "equipment", "materials"
    ]
    
    # Select table to manage
    selected_table = st.selectbox("Select Table", tables)
    
    # Fetch data from selected table
    table_data = session.table(selected_table).collect()
    if table_data:
        st.write(f"### {selected_table.capitalize()} Table")
        st.dataframe(table_data)
    
    # Add new record
    with st.expander(f"Add New Record to {selected_table}"):
        with st.form(f"add_{selected_table}_form"):
            # Dynamically create input fields based on table columns
            columns = session.table(selected_table).columns
            input_values = {}
            for col in columns:
                if col.lower().endswith("id"):  # Skip ID fields (auto-generated)
                    continue
                input_values[col] = st.text_input(f"{col}")
            
            if st.form_submit_button("Add Record"):
                try:
                    # Generate ID if not provided
                    if "id" in [c.lower() for c in columns]:
                        input_values[columns[0]] = f"{selected_table.upper()}_{datetime.now().timestamp()}"
                    
                    # Build SQL query
                    columns_str = ", ".join([f'"{col}"' for col in input_values.keys()])
                    values_str = ", ".join([f"'{input_values[col]}'" for col in input_values.keys()])
                    session.sql(f"""
                        INSERT INTO {selected_table} 
                        ({columns_str})
                        VALUES ({values_str})
                    """).collect()
                    st.success("Record added successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error adding record: {str(e)}")
    
    # Edit/Delete record
    with st.expander(f"Edit/Delete Record in {selected_table}"):
        if table_data:
            selected_record = st.selectbox(
                f"Select Record to Edit/Delete",
                options=[row[columns[0]] for row in table_data]
            )
            
            if selected_record:
                record_data = [row for row in table_data if row[columns[0]] == selected_record][0]
                
                with st.form(f"edit_{selected_table}_form"):
                    # Dynamically create input fields for editing
                    edit_values = {}
                    for col in columns:
                        if col.lower().endswith("id"):  # Skip ID fields (read-only)
                            st.text_input(f"{col} (Read-Only)", value=record_data[col], disabled=True)
                        else:
                            edit_values[col] = st.text_input(f"{col}", value=record_data[col])
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.form_submit_button("Update Record"):
                            try:
                                set_clause = ", ".join([f'"{col}" = \'{edit_values[col]}\'' for col in edit_values.keys()])
                                session.sql(f"""
                                    UPDATE {selected_table} 
                                    SET {set_clause}
                                    WHERE "{columns[0]}" = '{selected_record}'
                                """).collect()
                                st.success("Record updated successfully!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error updating record: {str(e)}")
                    with col2:
                        if st.form_submit_button("Delete Record"):
                            try:
                                session.sql(f"""
                                    DELETE FROM {selected_table} 
                                    WHERE "{columns[0]}" = '{selected_record}'
                                """).collect()
                                st.success("Record deleted successfully!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error deleting record: {str(e)}")
#######################################################################
# Main app function
def main_app():
    st.sidebar.title(f"Welcome {st.session_state.user_name}")

    # Available tabs based on roles
    available_tabs = set()
    for role in st.session_state.roles:
        available_tabs.update(ROLE_ACCESS.get(role.lower(), []))

    # Add the "Profile" tab for all employees
    available_tabs.add("profile")

    # Define tab order
    tab_order = ['home', 'profile', 'customers', 'appointments', 'quotes', 'jobs', 
                 'invoices', 'payments', 'reports', 'analytics', 'admin_tables', 'equipment']
    
    available_tabs = [tab for tab in tab_order if tab in available_tabs]

    # Sidebar navigation
    selected_tab = st.sidebar.selectbox("Navigation", available_tabs)

    if selected_tab == 'home':
        home()
    elif selected_tab == 'profile':
        profile_page()    
    elif selected_tab == 'customers':
        customer_management()
    elif selected_tab == 'appointments':
        appointments()
    elif selected_tab == 'quotes':
        quotes()
    elif selected_tab == 'jobs':
        jobs()
    elif selected_tab == 'invoices':
        invoices()
    elif selected_tab == 'payments':
        payments()
    elif selected_tab == 'reports':
        reports()
    elif selected_tab == 'analytics':
        analytics()
    elif selected_tab == 'admin_tables':
        admin_tables()
    elif selected_tab == 'equipment':
        equipment_management()
    

    # Logout button
    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.rerun()

# Main app flow
if __name__ == '__main__':
    query_params = st.query_params
    if 'reset_token' in query_params:
        reset_password(query_params['reset_token'])
    elif not st.session_state.get('logged_in'):
        login_page()
    else:
        main_app()
