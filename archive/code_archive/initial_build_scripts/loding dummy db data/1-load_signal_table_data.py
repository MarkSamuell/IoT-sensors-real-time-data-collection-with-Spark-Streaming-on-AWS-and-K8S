import psycopg2

# Connect to the PostgreSQL database
connection = psycopg2.connect(
   host="k8s-stg-postgres-08c0537730-dbdb8651674d2bce.elb.eu-central-1.amazonaws.com",
   port="5432",
   database="fleetconnect_db",
   user="postgres",
   password="mysecretpassword"
)

try:
    # Create a cursor object
    cursor = connection.cursor()
    
    # Insert sample data into the existing signal table
    cursor.execute("""
    INSERT INTO public.signal (
        name,
        description,
        min,
        max,
        component,
        unit,
        signal_fully_qualified_name,
        is_deleted,
        business_id
    )
    VALUES
        ('CPU_Load', 'CPU Load Signal', 0, 100, 'ECU', '%', 'Vehicle.Cabin.Infotainment.CPU.Load', false, '101'),
        ('Global_Temperature', 'Global Temperature Signal', -50, 80, 'Climate Control', 'Celsius', 'Vehicle.Cabin.HVAC.AmbientAirTemperature', false, '102'),
        ('Vehicle_Speed', 'Vehicle Speed Signal', 0, 300, 'Speedometer', 'km/h', 'Vehicle.Powertrain.Transmission.Speed', false, '103'),
        ('VSM EVENT OPMODE', 'VSM Event Operation Mode', NULL, NULL, NULL, NULL, '', false, '132608'),
        ('CODING EVENT NOT CODED', 'Coding Event Not Coded', NULL, NULL, NULL, NULL, '', false, '132616'),
        ('CODING EVENT WRONG VEHICLE', 'Coding Event Wrong Vehicle', NULL, NULL, NULL, NULL, '', false, '132617'),
        ('Local Over voltage', 'Local Over Voltage Event', NULL, NULL, NULL, NULL, '', false, '132672')
    """)
    print("Sample data inserted successfully into the signal table")
    
    # Commit changes
    connection.commit()
    
    # Verify the data
    cursor.execute("SELECT id, name, business_id, component, unit, min, max FROM public.signal WHERE is_deleted = false ORDER BY id DESC LIMIT 5;")
    rows = cursor.fetchall()
    print("\nLast 5 entries in the 'signal' table:")
    for row in rows:
        print(row)

except Exception as e:
    print(f"Error: {e}")
    connection.rollback()

finally:
    # Close cursor and connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()


        # ('Steering_Wheel_Angle', 'Steering Wheel Angle Signal', -540, 540, 'Steering System', 'degrees', 'Vehicle.Chassis.SteeringWheel.Angle', false, 'Vehicle.Chassis.SteeringWheel.Angle'),
        # ('Is_Park_Lock_Engaged', 'Park Lock Status', 0, 1, 'Transmission', 'boolean', 'Vehicle.Powertrain.Transmission.IsParkLockEngaged', false, 'Vehicle.Powertrain.Transmission.IsParkLockEngaged'),
        # ('Electric_Motor_Torque', 'Electric Motor Torque', -1000, 1000, 'Electric Motor', 'Nm', 'Vehicle.Powertrain.ElectricMotor.Torque', false, 'Vehicle.Powertrain.ElectricMotor.Torque'),
        # ('Control_Module_Voltage', 'Control Module Voltage', 0, 60, 'OBD System', 'V', 'Vehicle.OBD.ControlModuleVoltage', false, 'Vehicle.OBD.ControlModuleVoltage'),
        # ('Is_Charging', 'Battery Charging Status', 0, 1, 'Battery System', 'boolean', 'Vehicle.Powertrain.TractionBattery.Charging.IsCharging', false, 'Vehicle.Powertrain.TractionBattery.Charging.IsCharging'),
        # ('Is_Brake_Fluid_Level_Low', 'Brake Fluid Level Status', 0, 1, 'Braking System', 'boolean', 'Vehicle.Chassis.Axle.Row1.Wheel.Left.Brake.IsFluidLevelLow', false, 'Vehicle.Chassis.Axle.Row1.Wheel.Left.Brake.IsFluidLevelLow'),
        # ('Ambient_Air_Temperature', 'Ambient Air Temperature', -50, 60, 'OBD System', 'Celsius', 'Vehicle.OBD.AmbientAirTemperature', false, 'Vehicle.OBD.AmbientAirTemperature'),
        # ('Is_Washer_Fluid_Level_Low', 'Washer Fluid Level Status', 0, 1, 'Windshield System', 'boolean', 'Vehicle.Body.Windshield.Front.WasherFluid.IsLevelLow', false, 'Vehicle.Body.Windshield.Front.WasherFluid.IsLevelLow'),
        # ('Is_Left_Mirror_Heating_On', 'Left Mirror Heating Status', 0, 1, 'Mirror System', 'boolean', 'Vehicle.Body.Mirrors.Left.IsHeatingOn', false, 'Vehicle.Body.Mirrors.Left.IsHeatingOn'),
        # ('Left_Mirror_Tilt', 'Left Mirror Tilt Position', -90, 90, 'Mirror System', 'degrees', 'Vehicle.Body.Mirrors.Left.Tilt', false, 'Vehicle.Body.Mirrors.Left.Tilt'),
        # ('Left_Mirror_Pan', 'Left Mirror Pan Position', -90, 90, 'Mirror System', 'degrees', 'Vehicle.Body.Mirrors.Left.Pan', false, 'Vehicle.Body.Mirrors.Left.Pan'),
        # ('Right_Mirror_Tilt', 'Right Mirror Tilt Position', -90, 90, 'Mirror System', 'degrees', 'Vehicle.Body.Mirrors.Right.Tilt', false, 'Vehicle.Body.Mirrors.Right.Tilt'),
        # ('Right_Mirror_Pan', 'Right Mirror Pan Position', -90, 90, 'Mirror System', 'degrees', 'Vehicle.Body.Mirrors.Right.Pan', false, 'Vehicle.Body.Mirrors.Right.Pan'),
        # ('Is_Trunk_Open', 'Trunk Status', 0, 1, 'Trunk System', 'boolean', 'Vehicle.Body.Trunk.Rear.IsOpen', false, 'Vehicle.Body.Trunk.Rear.IsOpen'),
        # ('Electric_Motor_Temperature', 'Electric Motor Temperature', -50, 200, 'Electric Motor', 'Celsius', 'Vehicle.Powertrain.ElectricMotor.Temperature', false, 'Vehicle.Powertrain.ElectricMotor.Temperature'),
        # ('Is_Row1_Left_Door_Open', 'Row 1 Left Door Status', 0, 1, 'Door System', 'boolean', 'Vehicle.Cabin.Door.Row1.Left.IsOpen', false, 'Vehicle.Cabin.Door.Row1.Left.IsOpen'),
        # ('Is_Row2_Left_Door_Open', 'Row 2 Left Door Status', 0, 1, 'Door System', 'boolean', 'Vehicle.Cabin.Door.Row2.Left.IsOpen', false, 'Vehicle.Cabin.Door.Row2.Left.IsOpen'),
        # ('Is_Row1_Pos1_Seat_Belted', 'Row 1 Position 1 Seat Belt Status', 0, 1, 'Seat System', 'boolean', 'Vehicle.Cabin.Seat.Row1.Pos1.IsBelted', false, 'Vehicle.Cabin.Seat.Row1.Pos1.IsBelted'),
        # ('Is_Row1_Pos2_Seat_Occupied', 'Row 1 Position 2 Seat Occupancy', 0, 1, 'Seat System', 'boolean', 'Vehicle.Cabin.Seat.Row1.Pos2.IsOccupied', false, 'Vehicle.Cabin.Seat.Row1.Pos2.IsOccupied'),
        # ('Is_Brake_Light_On', 'Brake Light Status', 0, 1, 'Lighting System', 'boolean', 'Vehicle.Body.Lights.IsBrakeOn', false, 'Vehicle.Body.Lights.IsBrakeOn'),
        # ('Is_Rear_Fog_Light_On', 'Rear Fog Light Status', 0, 1, 'Lighting System', 'boolean', 'Vehicle.Body.Lights.IsRearFogOn', false, 'Vehicle.Body.Lights.IsRearFogOn'),
        # ('Is_Backup_Light_On', 'Backup Light Status', 0, 1, 'Lighting System', 'boolean', 'Vehicle.Body.Lights.IsBackupOn', false, 'Vehicle.Body.Lights.IsBackupOn'),
        # ('Is_Right_Indicator_On', 'Right Indicator Status', 0, 1, 'Lighting System', 'boolean', 'Vehicle.Body.Lights.IsRightIndicatorOn', false, 'Vehicle.Body.Lights.IsRightIndicatorOn'),
        # ('Is_Left_Indicator_On', 'Left Indicator Status', 0, 1, 'Lighting System', 'boolean', 'Vehicle.Body.Lights.IsLeftIndicatorOn', false, 'Vehicle.Body.Lights.IsLeftIndicatorOn'),
        # ('Is_Trailer_Connected', 'Trailer Connection Status', 0, 1, 'Trailer System', 'boolean', 'Vehicle.Trailer.IsConnected', false, 'Vehicle.Trailer.IsConnected'),
        # ('Engine_Load', 'Engine Load Percentage', 0, 100, 'OBD System', '%', 'Vehicle.OBD.EngineLoad', false, 'Vehicle.OBD.EngineLoad');