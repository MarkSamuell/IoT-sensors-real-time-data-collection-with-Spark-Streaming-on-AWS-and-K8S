import psycopg2
# Connect to the PostgreSQL database
connection = psycopg2.connect(
    host="127.0.0.1",
    port="9001",
    database="test_db",
    user="postgres",
    password="mysecretpassword"
)
try:
    # Create a cursor object
    cursor = connection.cursor()
   
    # Drop the cars_signals table if it exists to avoid conflicts
    cursor.execute("DROP TABLE IF EXISTS cars_signals;")
   
    # Create the cars_signals table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cars_signals (
        Signal_ID VARCHAR(255),    
        Signal_Name VARCHAR(255),
        Signal_Unit VARCHAR(255),
        Component VARCHAR(255),
        Min_Value FLOAT,
        Max_Value FLOAT,
        Signal_Fully_Qualified_Name VARCHAR(255)
    );
    """)
    print("Table 'cars_signals' created successfully")
   
    # Insert sample data for the three signals
    cursor.execute("""
    INSERT INTO cars_signals (Signal_ID, Signal_Name, Signal_Unit, Component, Min_Value, Max_Value, Signal_Fully_Qualified_Name)
    VALUES
    ('101', 'CPU_Load', '%', 'ECU', 0, 100, 'Vehicle.Cabin.Infotainment.CPU.Load'),
    ('102', 'Global_Temperature', 'Celsius', 'Climate Control', -50, 80, 'Vehicle.Cabin.HVAC.AmbientAirTemperature'),
    ('103', 'Vehicle_Speed', 'km/h', 'Speedometer', 0, 300, 'Vehicle.Powertrain.Transmission.Speed'),
    ('132608', 'VSM EVENT OPMODE', NULL, NULL, NULL, NULL, NULL),
    ('132616', 'CODING EVENT NOT CODED', NULL, NULL, NULL, NULL, NULL),
    ('132617', 'CODING EVENT WRONG VEHICLE', NULL, NULL, NULL, NULL, NULL),
    ('132672', 'Local Over voltage', NULL, NULL, NULL, NULL, NULL),
    ('Vehicle.Chassis.SteeringWheel.Angle', 'Steering_Wheel_Angle', 'degrees', 'Steering System', -540, 540, 'Vehicle.Chassis.SteeringWheel.Angle'),
    ('Vehicle.Speed', 'Vehicle_Speed', 'km/h', 'Speedometer', 0, 300, 'Vehicle.Speed'),
    ('Vehicle.Powertrain.Transmission.IsParkLockEngaged', 'Is_Park_Lock_Engaged', 'boolean', 'Transmission', 0, 1, 'Vehicle.Powertrain.Transmission.IsParkLockEngaged'),
    ('Vehicle.Powertrain.ElectricMotor.Torque', 'Electric_Motor_Torque', 'Nm', 'Electric Motor', -1000, 1000, 'Vehicle.Powertrain.ElectricMotor.Torque'),
    ('Vehicle.OBD.ControlModuleVoltage', 'Control_Module_Voltage', 'V', 'OBD System', 0, 60, 'Vehicle.OBD.ControlModuleVoltage'),
    ('Vehicle.Powertrain.TractionBattery.Charging.IsCharging', 'Is_Charging', 'boolean', 'Battery System', 0, 1, 'Vehicle.Powertrain.TractionBattery.Charging.IsCharging'),
    ('Vehicle.Chassis.Axle.Row1.Wheel.Left.Brake.IsFluidLevelLow', 'Is_Brake_Fluid_Level_Low', 'boolean', 'Braking System', 0, 1, 'Vehicle.Chassis.Axle.Row1.Wheel.Left.Brake.IsFluidLevelLow'),
    ('Vehicle.OBD.AmbientAirTemperature', 'Ambient_Air_Temperature', 'Celsius', 'OBD System', -50, 60, 'Vehicle.OBD.AmbientAirTemperature'),
    ('Vehicle.Body.Windshield.Front.WasherFluid.IsLevelLow', 'Is_Washer_Fluid_Level_Low', 'boolean', 'Windshield System', 0, 1, 'Vehicle.Body.Windshield.Front.WasherFluid.IsLevelLow'),
    ('Vehicle.Body.Mirrors.Left.IsHeatingOn', 'Is_Left_Mirror_Heating_On', 'boolean', 'Mirror System', 0, 1, 'Vehicle.Body.Mirrors.Left.IsHeatingOn'),
    ('Vehicle.Body.Mirrors.Left.Tilt', 'Left_Mirror_Tilt', 'degrees', 'Mirror System', -90, 90, 'Vehicle.Body.Mirrors.Left.Tilt'),
    ('Vehicle.Body.Mirrors.Left.Pan', 'Left_Mirror_Pan', 'degrees', 'Mirror System', -90, 90, 'Vehicle.Body.Mirrors.Left.Pan'),
    ('Vehicle.Body.Mirrors.Right.Tilt', 'Right_Mirror_Tilt', 'degrees', 'Mirror System', -90, 90, 'Vehicle.Body.Mirrors.Right.Tilt'),
    ('Vehicle.Body.Mirrors.Right.Pan', 'Right_Mirror_Pan', 'degrees', 'Mirror System', -90, 90, 'Vehicle.Body.Mirrors.Right.Pan'),
    ('Vehicle.Body.Trunk.Rear.IsOpen', 'Is_Trunk_Open', 'boolean', 'Trunk System', 0, 1, 'Vehicle.Body.Trunk.Rear.IsOpen'),
    ('Vehicle.Powertrain.ElectricMotor.Temperature', 'Electric_Motor_Temperature', 'Celsius', 'Electric Motor', -50, 200, 'Vehicle.Powertrain.ElectricMotor.Temperature'),
    ('Vehicle.Cabin.Door.Row1.Left.IsOpen', 'Is_Row1_Left_Door_Open', 'boolean', 'Door System', 0, 1, 'Vehicle.Cabin.Door.Row1.Left.IsOpen'),
    ('Vehicle.Cabin.Door.Row2.Left.IsOpen', 'Is_Row2_Left_Door_Open', 'boolean', 'Door System', 0, 1, 'Vehicle.Cabin.Door.Row2.Left.IsOpen'),
    ('Vehicle.Cabin.Seat.Row1.Pos1.IsBelted', 'Is_Row1_Pos1_Seat_Belted', 'boolean', 'Seat System', 0, 1, 'Vehicle.Cabin.Seat.Row1.Pos1.IsBelted'),
    ('Vehicle.Cabin.Seat.Row1.Pos2.IsOccupied', 'Is_Row1_Pos2_Seat_Occupied', 'boolean', 'Seat System', 0, 1, 'Vehicle.Cabin.Seat.Row1.Pos2.IsOccupied'),
    ('Vehicle.Body.Lights.IsBrakeOn', 'Is_Brake_Light_On', 'boolean', 'Lighting System', 0, 1, 'Vehicle.Body.Lights.IsBrakeOn'),
    ('Vehicle.Body.Lights.IsRearFogOn', 'Is_Rear_Fog_Light_On', 'boolean', 'Lighting System', 0, 1, 'Vehicle.Body.Lights.IsRearFogOn'),
    ('Vehicle.Body.Lights.IsBackupOn', 'Is_Backup_Light_On', 'boolean', 'Lighting System', 0, 1, 'Vehicle.Body.Lights.IsBackupOn'),
    ('Vehicle.Body.Lights.IsRightIndicatorOn', 'Is_Right_Indicator_On', 'boolean', 'Lighting System', 0, 1, 'Vehicle.Body.Lights.IsRightIndicatorOn'),
    ('Vehicle.Body.Lights.IsLeftIndicatorOn', 'Is_Left_Indicator_On', 'boolean', 'Lighting System', 0, 1, 'Vehicle.Body.Lights.IsLeftIndicatorOn'),
    ('Vehicle.Trailer.IsConnected', 'Is_Trailer_Connected', 'boolean', 'Trailer System', 0, 1, 'Vehicle.Trailer.IsConnected'),
    ('Vehicle.OBD.EngineLoad', 'Engine_Load', '%', 'OBD System', 0, 100, 'Vehicle.OBD.EngineLoad');
    """)
    print("Sample data inserted successfully")
   
    # Commit changes
    connection.commit()
   
    # Verify the data
    cursor.execute("SELECT * FROM cars_signals;")
    rows = cursor.fetchall()
    print("\nContents of 'cars_signals' table:")
    for row in rows:
        print(row)
   
except Exception as e:
    print(f"Error: {e}")
   
finally:
    # Close cursor and connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()