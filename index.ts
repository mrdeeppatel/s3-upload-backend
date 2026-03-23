import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import fs from "fs";
import path from "path";
import { pipeline } from "stream/promises";
import nodemailer from "nodemailer";
import AdmZip from "adm-zip";
import csvParser from "csv-parser";

import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";

import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

// 🔹 S3 Client
const s3 = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  } as any,
});
app.post("/retry-mails", async (req, res) => {
  try {
    const { failedUsers, extractPath, email, pass } = req.body;


    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: { user: email, pass },
      pool: true,
      maxConnections: 5,
      maxMessages: 100,
    });

    const results = [];

    for (const user of failedUsers) {
      const safeName = user.name.trim().replace(/\s+/g, "_");
      const fileName = `${user.id}_${safeName}.pdf`;
      const filePath = path.join(extractPath, fileName);

      try {
        await transporter.sendMail({
          from: email,
          to: user.email,
          subject: "Retry: Certificate",
          text: `Hi ${user.name}, retrying your certificate.`,
          attachments: [{ filename: fileName, path: filePath }],
        });

        results.push({ email: user.email, status: "sent" });

      } catch {
        results.push({ email: user.email, status: "failed again" });
      }
    }

    res.json({ results });


  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Retry failed" });
  }
});


// ==========================
// ✅ Get Upload URL
// ==========================
const buildFileName = (row: any, nameParts: any) => {
  const parts = nameParts.map((part: any) => {
    if (part.type === "column") {
      return row[part.value] || "";
    } else {
      return part.value;
    }
  });

  return parts
    .filter(Boolean)
    .join("_")
    .replace(/[^a-z0-9_]/gi, "_")
    .toLowerCase();
};

app.post("/get-upload-url", async (req, res) => {
  try {
    const { fileName, fileType } = req.body;


    const key = `uploads/${Date.now()}-${fileName}`;

    const command = new PutObjectCommand({
      Bucket: process.env.AWS_BUCKET_NAME,
      Key: key,
      ContentType: fileType,
    });

    const uploadUrl = await getSignedUrl(s3, command, {
      expiresIn: 60,
    });

    const fileUrl = `https://${process.env.AWS_BUCKET_NAME}.s3.${process.env.AWS_REGION}.amazonaws.com/${key}`;

    res.json({ uploadUrl, fileUrl });


  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to generate URL" });
  }
});

// ==========================
// 🔥 Process ZIP + CSV + Send Mail
// ==========================
app.post("/process-file", async (req, res) => {
  try {
    const { zipKey, csvKey, email, pass, nameParts } = req.body;


    if (!zipKey || !csvKey || !email || !pass || !nameParts) {
      return res.status(400).json({ error: "Missing data" });
    }

    fs.mkdirSync("downloads", { recursive: true });

    // ==========================
    // 📥 1. Download ZIP
    // ==========================
    const zipResponse = await s3.send(
      new GetObjectCommand({
        Bucket: process.env.AWS_BUCKET_NAME,
        Key: zipKey,
      })
    );

    const zipPath = path.join("downloads", `${Date.now()}.zip`);
    await pipeline(zipResponse.Body as any, fs.createWriteStream(zipPath));

    // ==========================
    // 📦 2. Extract ZIP
    // ==========================
    const extractPath = path.join("downloads", `extracted_${Date.now()}`);
    fs.mkdirSync(extractPath);

    const zip = new AdmZip(zipPath);
    zip.extractAllTo(extractPath, true);

    const fileSet = new Set(fs.readdirSync(extractPath));

    // ==========================
    // 📥 3. Download CSV
    // ==========================
    const csvResponse = await s3.send(
      new GetObjectCommand({
        Bucket: process.env.AWS_BUCKET_NAME,
        Key: csvKey,
      })
    );

    const csvPath = path.join("downloads", `${Date.now()}.csv`);
    await pipeline(csvResponse.Body as any, fs.createWriteStream(csvPath));

    // ==========================
    // 📄 4. Parse CSV
    // ==========================
    const users: any[] = [];

    await new Promise((resolve, reject) => {
      fs.createReadStream(csvPath)
        .pipe(csvParser())
        .on("data", (row) => users.push(row))
        .on("end", resolve)
        .on("error", reject);
    });

    console.log("👥 Users:", users);

    // ==========================
    // 📊 5. Progress object
    // ==========================
    let progress = {
      total: users.length,
      sent: 0,
      failed: [] as any[],
    };

    // ==========================
    // 📧 6. Mail transporter
    // ==========================
    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: email,
        pass: pass,
      },
    });

    // ==========================
    // 🔁 7. Process users
    // ==========================
    const BATCH_SIZE = 5;

    for (let i = 0; i < users.length; i += BATCH_SIZE) {
      const batch = users.slice(i, i + BATCH_SIZE);

      await Promise.all(
        batch.map(async (user) => {
          const fileBaseName = buildFileName(user, nameParts);
          const expectedFile = `${fileBaseName}.pdf`;

          if (!fileSet.has(expectedFile)) {
            progress.failed.push({ ...user, reason: "file not found" });
            return;
          }

          try {
            await transporter.sendMail({
              from: email,
              to: user.email,
              subject: "Your Certificate",
              text: `Hi ${user.name}`,
              attachments: [
                {
                  filename: expectedFile,
                  path: path.join(extractPath, expectedFile),
                },
              ],
            });

            progress.sent++;
          } catch {
            progress.failed.push({ ...user, reason: "mail failed" });
          }
        })
      );

      console.log(`Batch ${i / BATCH_SIZE + 1} done`);
    }
    // ==========================
    // 🎉 Final response
    // ==========================
    res.json({
      message: "Process completed",
      progress,
      extractPath, // 🔥 needed for retry
    });


  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Processing failed" });
  }
});

app.listen(process.env.PORT, () => {
  console.log(`🚀 Server running on port ${process.env.PORT}`);
});
