/// <reference path="../typings/index.d.ts"/>

import { BufferReader, BufferWriter } from "./buffer-utils";
import { FIREHOSE_ENVELOPE } from "./envelope-structure";
import * as uuid from "node-uuid";
import { KinesisBridgeEnvelope } from "./kinesis-bridge-envelope";

export class KinesisBridgeEnvelopeParser {
  /**
   * Parses a base64 encoded Kinesis Bridge envelope string payload
   * @param base64Data
   * @returns {*}
   */
  public parseData(base64Data: string): KinesisBridgeEnvelope {
    const reader = new BufferReader(new Buffer(base64Data, "base64"));
    const envelope = new KinesisBridgeEnvelope();
    envelope.headerVersion = reader.readIntLE(FIREHOSE_ENVELOPE.HEADER_VERSION);
    envelope.timeUUID = uuid.unparse(
      reader.readBuffer(FIREHOSE_ENVELOPE.TIME_UUID)
    );
    envelope.sourceNameLength = reader.readMultibyteNumLE(
      FIREHOSE_ENVELOPE.SOURCE_NAME_LENGTH
    );
    envelope.sourceName = reader.readString(envelope.sourceNameLength);
    envelope.sourcePropertiesLength = reader.readMultibyteNumLE(
      FIREHOSE_ENVELOPE.SOURCE_PROPERTIES_LENGTH
    );
    const serializedSourceProperties = reader.readString(
      envelope.sourcePropertiesLength
    );
    if (serializedSourceProperties.length > 0) {
      envelope.sourceProperties = JSON.parse(serializedSourceProperties);
    } else {
      envelope.sourceProperties = {};
    }

    envelope.targetNameLength = reader.readMultibyteNumLE(
      FIREHOSE_ENVELOPE.TARGET_NAME_LENGTH
    );
    envelope.targetName = reader.readString(envelope.targetNameLength);
    envelope.targetPropertiesLength = reader.readMultibyteNumLE(
      FIREHOSE_ENVELOPE.TARGET_PROPERTIES_LENGTH
    );
    envelope.targetProperties = JSON.parse(
      reader.readString(envelope.targetPropertiesLength)
    );
    envelope.contentLength = reader.readMultibyteNumLE(
      FIREHOSE_ENVELOPE.CONTENT_LENGTH
    );
    envelope.contentBody = reader.readBuffer(envelope.contentLength);
    return envelope;
  }

  public encode(envelope: KinesisBridgeEnvelope): Buffer {
    let bodyString;

    if (typeof envelope.contentBody === "string") {
      envelope.contentBody = JSON.parse(envelope.contentBody);
    }

    bodyString = JSON.stringify(envelope.contentBody);

    envelope.contentLength = Buffer.byteLength(bodyString, "utf8");
    if (!envelope.sourceProperties) {
      envelope.sourceProperties = {};
    }
    let sourcePropertiesString = JSON.stringify(envelope.sourceProperties);
    envelope.sourcePropertiesLength = Buffer.byteLength(
      sourcePropertiesString,
      "utf8"
    );
    let timeString = Buffer.from(uuid.parse(envelope.timeUUID)).toString(
      "utf8"
    );
    let targetPropertiesString = JSON.stringify(envelope.targetProperties);
    envelope.targetPropertiesLength = Buffer.byteLength(
      targetPropertiesString,
      "utf8"
    );

    let bufsize =
      FIREHOSE_ENVELOPE.HEADER_VERSION +
      FIREHOSE_ENVELOPE.TIME_UUID +
      FIREHOSE_ENVELOPE.SOURCE_NAME_LENGTH +
      FIREHOSE_ENVELOPE.SOURCE_PROPERTIES_LENGTH +
      FIREHOSE_ENVELOPE.TARGET_NAME_LENGTH +
      FIREHOSE_ENVELOPE.TARGET_PROPERTIES_LENGTH +
      FIREHOSE_ENVELOPE.CONTENT_LENGTH +
      envelope.contentLength +
      Buffer.byteLength(envelope.sourceName, "utf8") +
      envelope.sourcePropertiesLength +
      Buffer.byteLength(envelope.targetName, "utf8") +
      envelope.targetPropertiesLength;

    const writer = new BufferWriter(Buffer.alloc(bufsize));
    writer.writeIntLE(envelope.headerVersion, FIREHOSE_ENVELOPE.HEADER_VERSION);
    writer.writeString(timeString, FIREHOSE_ENVELOPE.TIME_UUID);
    writer.writeMultibyteNumLE(
      Buffer.byteLength(envelope.sourceName, "utf8"),
      FIREHOSE_ENVELOPE.SOURCE_NAME_LENGTH
    );
    writer.writeString(
      envelope.sourceName,
      Buffer.byteLength(envelope.sourceName, "utf8")
    );
    writer.writeMultibyteNumLE(
      envelope.sourcePropertiesLength,
      FIREHOSE_ENVELOPE.SOURCE_PROPERTIES_LENGTH
    );

    writer.writeString(sourcePropertiesString, envelope.sourcePropertiesLength);

    writer.writeMultibyteNumLE(
      Buffer.byteLength(envelope.targetName, "utf8"),
      FIREHOSE_ENVELOPE.TARGET_NAME_LENGTH
    );

    writer.writeString(
      envelope.targetName,
      Buffer.byteLength(envelope.targetName, "utf8")
    );

    writer.writeMultibyteNumLE(
      envelope.targetPropertiesLength,
      FIREHOSE_ENVELOPE.TARGET_PROPERTIES_LENGTH
    );
    writer.writeString(targetPropertiesString, envelope.targetPropertiesLength);

    writer.writeMultibyteNumLE(
      envelope.contentLength,
      FIREHOSE_ENVELOPE.CONTENT_LENGTH
    );

    writer.writeString(bodyString, envelope.contentLength);

    let ret = writer.getBuffer();
    return ret;
  }
}
