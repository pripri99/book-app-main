const { Pool } = require("pg");

// Create the books table if it does not exist
async function createBooksTable(pool) {
  const client = await pool.connect();
  try {
    await client.query(`
        CREATE TABLE IF NOT EXISTS books (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          author TEXT NOT NULL,
          isbn TEXT NOT NULL UNIQUE
        );
      `);
    console.log("Created books table");
  } catch (error) {
    console.error("Error creating books table:", error);
  } finally {
    client.release();
  }
}

const main = async ({ dataSourceLinks, data, dataSinkLinks, credentials }) => {
  // Initialize the PostgreSQL connection pool
  const pool = new Pool(credentials);

  try {
    // Create the books table if it does not exist
    await createBooksTable(pool);

    // Insert the book into the database
    const { title, author, isbn } = data;
    const query = `
      INSERT INTO books (title, author, isbn)
      VALUES ($1, $2, $3)
      RETURNING *;
    `;
    const values = [title, author, isbn];
    const result = await pool.query(query, values);

    // Close the connection pool
    await pool.end();

    return {
      success: true,
      message: "Book added successfully",
      data: result.rows[0],
    };
  } catch (error) {
    // Close the connection pool in case of an error
    await pool.end();

    return {
      success: false,
      message: "Failed to add book ",
      error: error.message,
    };
  }
};

module.exports = main;
