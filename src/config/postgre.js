const { Pool } = require("pg");
const dotenv = require("dotenv");
const bcrypt = require("bcrypt");

dotenv.config();

const pool = new Pool({
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  host: process.env.DB_HOST,
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME,
});

const checkDatabaseConnection = async () => {
  try {
    await pool.query("SELECT 1");
    console.log("✅ Database connected successfully");
  } catch (err) {
    console.error("❌ Database connection failed:", err.message);
    process.exit(1);
  }
};

const DEFAULT_SUPERADMIN_EMAIL = "admin@example.com";
const DEFAULT_SUPERADMIN_NAME = "Super Admin";

async function checkAndCreateTables() {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const tableDDLs = {
      articles: `
        CREATE TABLE IF NOT EXISTS public.articles (
            id uuid DEFAULT gen_random_uuid() NOT NULL,
            title varchar(255) NOT NULL,
            content text NOT NULL,
            header_image_id uuid NULL,
            author_id uuid NULL,
            is_published bool DEFAULT false NULL,
            created_at timestamptz DEFAULT now() NULL,
            updated_at timestamptz DEFAULT now() NULL,
            CONSTRAINT articles_pkey PRIMARY KEY (id)
        );
      `,
      onboarding_devices: `
        CREATE TABLE IF NOT EXISTS public.onboarding_devices (
            id uuid DEFAULT gen_random_uuid() NOT NULL,
            name text NOT NULL,
            location text NOT NULL,
            user_id uuid NOT NULL,
            status text NOT NULL,
            created_at timestamptz DEFAULT now() NULL,
            updated_at timestamptz DEFAULT now() NULL,
            data_interval_seconds int4 NULL,
            CONSTRAINT onboarding_devices_pkey PRIMARY KEY (id),
            CONSTRAINT onboarding_devices_status_check CHECK ((status = ANY (ARRAY['active'::text, 'inactive'::text])))
        );
      `,
      users: `
        CREATE TABLE IF NOT EXISTS public.users (
            id uuid DEFAULT gen_random_uuid() NOT NULL,
            name varchar(255) NOT NULL,
            email varchar(255) NOT NULL UNIQUE,
            password_hash varchar(255) NOT NULL,
            role varchar(50) NOT NULL DEFAULT 'user',
            is_active bool NOT NULL DEFAULT false,
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
            verification_code varchar NULL,
            CONSTRAINT users_email_key UNIQUE (email),
            CONSTRAINT users_pkey PRIMARY KEY (id)
        );
      `,
      sensor_types: `
        CREATE TABLE IF NOT EXISTS public.sensor_types (
            id uuid DEFAULT gen_random_uuid() NOT NULL,
            name text NOT NULL UNIQUE,
            unit text NULL,
            description text NULL,
            CONSTRAINT sensor_types_name_key UNIQUE (name),
            CONSTRAINT sensor_types_pkey PRIMARY KEY (id)
        );
      `,
      device_sensors: `
        CREATE TABLE IF NOT EXISTS public.device_sensors (
            id uuid DEFAULT gen_random_uuid() NOT NULL,
            device_id uuid NOT NULL,
            sensor_type_id uuid NOT NULL,
            created_at timestamptz DEFAULT now() NULL,
            CONSTRAINT device_sensors_device_id_sensor_type_id_key UNIQUE (device_id, sensor_type_id),
            CONSTRAINT device_sensors_pkey PRIMARY KEY (id),
            CONSTRAINT device_sensors_device_id_fkey FOREIGN KEY (device_id) REFERENCES public.onboarding_devices(id) ON DELETE CASCADE,
            CONSTRAINT device_sensors_sensor_type_id_fkey FOREIGN KEY (sensor_type_id) REFERENCES public.sensor_types(id) ON DELETE CASCADE
        );
      `,
      refresh_tokens: `
        CREATE TABLE IF NOT EXISTS public.refresh_tokens (
            id uuid DEFAULT gen_random_uuid() NOT NULL,
            user_id uuid NULL,
            refresh_token text NOT NULL,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
            expires_at timestamp NOT NULL,
            is_revoked bool DEFAULT false NULL,
            CONSTRAINT refresh_tokens_pkey PRIMARY KEY (id),
            CONSTRAINT refresh_tokens_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE
        );
      `,
      server_errors: `
        CREATE TABLE IF NOT EXISTS public.server_errors (
            id serial4 NOT NULL,
            message text NOT NULL,
            stack text NULL,
            request_url text NULL,
            request_method varchar(10) NULL,
            request_body jsonb NULL,
            created_at timestamp DEFAULT now() NULL,
            CONSTRAINT server_errors_pkey PRIMARY KEY (id)
        );
      `,
      warnings: `
        CREATE TABLE IF NOT EXISTS public.warnings (
            id uuid DEFAULT gen_random_uuid() NOT NULL,
            message varchar(500) NOT NULL,
            type varchar(50) DEFAULT 'general' NULL,
            is_active bool DEFAULT true NULL,
            source varchar(20) DEFAULT 'manual' NULL,
            hazard varchar(50) NULL,
            level varchar(20) NULL,
            created_at timestamptz DEFAULT now() NULL,
            updated_at timestamptz DEFAULT now() NULL,
            CONSTRAINT warnings_pkey PRIMARY KEY (id)
        );
      `,
      recent_activities: `
        CREATE TABLE IF NOT EXISTS public.recent_activities (
            id uuid DEFAULT gen_random_uuid() NOT NULL,
            message text NOT NULL,
            created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT recent_activities_pkey PRIMARY KEY (id)
        );
      `,
    };

    for (const ddl of Object.values(tableDDLs)) {
      await client.query(ddl);
    }

    // Idempotent migration for deployments created before the structured fields existed.
    await client.query(`
      ALTER TABLE public.warnings
        ADD COLUMN IF NOT EXISTS source varchar(20) DEFAULT 'manual',
        ADD COLUMN IF NOT EXISTS hazard varchar(50),
        ADD COLUMN IF NOT EXISTS level varchar(20);
    `);

    const email = process.env.SUPERADMIN_EMAIL || DEFAULT_SUPERADMIN_EMAIL;
    const rawPassword = process.env.SUPERADMIN_PASSWORD;
    const name = process.env.SUPERADMIN_NAME || DEFAULT_SUPERADMIN_NAME;

    const countRes = await client.query(
      `SELECT COUNT(*) FROM public.users WHERE role = 'superAdmin';`
    );
    const exists = parseInt(countRes.rows[0].count, 10) > 0;

    if (!exists) {
      if (!rawPassword) {
        // Never fall back to a baked-in default: the credential must come from env.
        console.warn(
          "⚠️ SUPERADMIN_PASSWORD is not set — skipping superadmin seed."
        );
      } else {
        const passwordHash = await bcrypt.hash(rawPassword, 10);

        await client.query(
          `INSERT INTO public.users (name, email, password_hash, role, is_active)
           VALUES ($1, $2, $3, 'superAdmin', true);`,
          [name, email, passwordHash]
        );
        console.log(`✅ Superadmin created: ${email}`);
      }
    } else {
      console.log("🔐 Superadmin already exists. Skipping creation.");
    }

    await client.query("COMMIT");
    console.log("✅ Migration and superAdmin seed completed successfully");
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("❌ Migration failed, rolled back:", error);
    process.exit(1);
  } finally {
    client.release();
  }
}

module.exports = { pool, checkDatabaseConnection, checkAndCreateTables };
