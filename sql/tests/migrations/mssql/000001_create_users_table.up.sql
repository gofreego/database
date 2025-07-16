IF OBJECT_ID('posts', 'U') IS NOT NULL
    DROP TABLE posts;

IF OBJECT_ID('users', 'U') IS NOT NULL
    DROP TABLE users;

CREATE TABLE users (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255) NOT NULL,
    email NVARCHAR(255) NOT NULL UNIQUE,
    password_hash NVARCHAR(255) NOT NULL,
    is_active INT DEFAULT 1,
    created_at BIGINT DEFAULT 0,
    updated_at BIGINT DEFAULT 0
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_active ON users(is_active);

CREATE TABLE posts (
    id INT IDENTITY(1,1) PRIMARY KEY,
    user_id INT NOT NULL,
    title NVARCHAR(255) NOT NULL,
    content NTEXT,
    status NVARCHAR(50) DEFAULT 'draft',
    published_at DATETIME2 NULL,
    created_at BIGINT DEFAULT 0,
    updated_at BIGINT DEFAULT 0,
    CONSTRAINT FK_posts_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_status ON posts(status);
CREATE INDEX idx_posts_published_at ON posts(published_at); 