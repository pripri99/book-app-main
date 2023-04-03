const { Pool } = require("pg");

const main = async ({ dataSourceLinks, data, dataSinkLinks, credentials }) => {
  // Initialize the PostgreSQL connection pool
  const pool = new Pool(credentials);

  try {
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
      message: "Failed to add book",
      error: error.message,
    };
  }
};

module.exports = main;
