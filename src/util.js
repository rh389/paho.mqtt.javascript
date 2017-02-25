/* @flow */

import { ERROR, MESSAGE_TYPE } from './constants';
import WireMessage from './WireMessage';
import PublishMessage from './PublishMessage';
import Message from './Message';

export function invariant(condition: boolean, message: string) {
  if (!condition) {
    throw new Error(message);
  }
}

/**
 * Format an error message text.
 *
 * @param error ERROR.KEY value above.
 * @param substitutions [array] substituted into the text.
 * @return the text with the substitutions made.
 */
export function format(error: { text: string }, substitutions?: (?(string | number | Uint8Array))[]) {
  let text = error.text;
  if (substitutions) {
    let field;
    substitutions.forEach((substitution, i) => {
      if (substitution === null) {
        substitution = 'null';
      }
      if (substitution === undefined) {
        substitution = 'undefined';
      }
      field = '{' + i + '}';
      text = text.replace(field, substitution.toString());
    });
  }
  return text;
}

/**
 * Validate an object's parameter names to ensure they
 * match a list of expected letiables name for this option
 * type. Used to ensure option object passed into the API don't
 * contain erroneous parameters.
 * @param {Object} obj - User options object
 * @param {Object} keys - valid keys and types that may exist in obj.
 * @throws {Error} Invalid option parameter found.
 * @private
 */
export function validate(obj: {}, keys: { [key: string]: string }) {
  Object.keys(obj).forEach(key => {
    if (keys.hasOwnProperty(key)) {
      let desiredType = keys[key];
      if (!(desiredType.indexOf('?') === 0 || (typeof obj[key] === 'undefined' || obj[key] === null))) {
        if (typeof obj[key] !== desiredType) {
          throw new Error(format(ERROR.INVALID_TYPE, [typeof obj[key], key]));
        }
      }
    } else {
      throw new Error('Unknown property, ' + key + '. Valid properties are: ' + Object.keys(keys).join(' '));
    }
  });
}

//Write a 16-bit number into two bytes of Uint8Array, starting at offset
export function writeUint16(input: number, buffer: Uint8Array, offset: number): number {
  buffer[offset++] = input >> 8;      //MSB
  buffer[offset++] = input % 256;     //LSB
  return offset;
}

export function writeString(input: string, utf8Length: number, buffer: Uint8Array, offset: number) {
  offset = writeUint16(utf8Length, buffer, offset);
  stringToUTF8(input, buffer, offset);
  return offset + utf8Length;
}

//Read a 16-bit number out of two bytes of a Uint8Array
export function readUint16(buffer: Uint8Array, offset: number): number {
  return 256 * buffer[offset] + buffer[offset + 1];
}

/**
 * Encodes an MQTT Multi-Byte Integer
 * @private
 */
export function encodeMultiByteInteger(number: number) {
  let output = new Array(1);
  let numBytes = 0;

  do {
    let digit = number % 128;
    number = number >> 7;
    if (number > 0) {
      digit |= 0x80;
    }
    output[numBytes++] = digit;
  } while ((number > 0) && (numBytes < 4));

  return output;
}

/**
 * Takes a String and calculates its length in bytes when encoded in UTF8.
 * @private
 */
export function lengthOfUTF8(input: string): number {
  let output = 0;
  for (let i = 0; i < input.length; i++) {
    const charCode = input.charCodeAt(i);
    if (charCode > 0x7FF) {
      // Surrogate pair means its a 4 byte character
      if (charCode >= 0xD800 && charCode <= 0xDBFF) {
        i++;
        output++;
      }
      output += 3;
    }
    else if (charCode > 0x7F) {
      output += 2;
    } else {
      output++;
    }
  }
  return output;
}

/**
 * Takes a String and writes it into an array as UTF8 encoded bytes.
 * @private
 */
export function stringToUTF8(input: string, output: Uint8Array, start: number): Uint8Array {
  let pos = start;
  for (let i = 0; i < input.length; i++) {
    let charCode = input.charCodeAt(i);

    // Check for a surrogate pair.
    if (charCode >= 0xD800 && charCode <= 0xDBFF) {
      const lowCharCode = input.charCodeAt(++i);
      if (isNaN(lowCharCode)) {
        throw new Error(format(ERROR.MALFORMED_UNICODE, [charCode, lowCharCode]));
      }
      charCode = ((charCode - 0xD800) << 10) + (lowCharCode - 0xDC00) + 0x10000;

    }

    if (charCode <= 0x7F) {
      output[pos++] = charCode;
    } else if (charCode <= 0x7FF) {
      output[pos++] = charCode >> 6 & 0x1F | 0xC0;
      output[pos++] = charCode & 0x3F | 0x80;
    } else if (charCode <= 0xFFFF) {
      output[pos++] = charCode >> 12 & 0x0F | 0xE0;
      output[pos++] = charCode >> 6 & 0x3F | 0x80;
      output[pos++] = charCode & 0x3F | 0x80;
    } else {
      output[pos++] = charCode >> 18 & 0x07 | 0xF0;
      output[pos++] = charCode >> 12 & 0x3F | 0x80;
      output[pos++] = charCode >> 6 & 0x3F | 0x80;
      output[pos++] = charCode & 0x3F | 0x80;
    }
  }
  return output;
}

export function parseUTF8(input: Uint8Array, offset: number, length: number): string {
  let output = '';
  let utf16;
  let pos = offset;

  while (pos < offset + length) {
    let byte1 = input[pos++];
    if (byte1 < 128) {
      utf16 = byte1;
    } else {
      let byte2 = input[pos++] - 128;
      if (byte2 < 0) {
        throw new Error(format(ERROR.MALFORMED_UTF, [byte1.toString(16), byte2.toString(16), '']));
      }
      if (byte1 < 0xE0)             // 2 byte character
      {
        utf16 = 64 * (byte1 - 0xC0) + byte2;
      } else {
        let byte3 = input[pos++] - 128;
        if (byte3 < 0) {
          throw new Error(format(ERROR.MALFORMED_UTF, [byte1.toString(16), byte2.toString(16), byte3.toString(16)]));
        }
        if (byte1 < 0xF0)        // 3 byte character
        {
          utf16 = 4096 * (byte1 - 0xE0) + 64 * byte2 + byte3;
        } else {
          let byte4 = input[pos++] - 128;
          if (byte4 < 0) {
            throw new Error(format(ERROR.MALFORMED_UTF, [byte1.toString(16), byte2.toString(16), byte3.toString(16), byte4.toString(16)]));
          }
          if (byte1 < 0xF8)        // 4 byte character
          {
            utf16 = 262144 * (byte1 - 0xF0) + 4096 * byte2 + 64 * byte3 + byte4;
          } else                     // longer encodings are not supported
          {
            throw new Error(format(ERROR.MALFORMED_UTF, [byte1.toString(16), byte2.toString(16), byte3.toString(16), byte4.toString(16)]));
          }
        }
      }
    }

    if (utf16 > 0xFFFF)   // 4 byte character - express as a surrogate pair
    {
      utf16 -= 0x10000;
      output += String.fromCharCode(0xD800 + (utf16 >> 10)); // lead character
      utf16 = 0xDC00 + (utf16 & 0x3FF);  // trail character
    }
    output += String.fromCharCode(utf16);
  }
  return output;
}

export function decodeMessage(input: Uint8Array, pos: number): [?WireMessage | PublishMessage, number] {
  const startingPos = pos;
  let first = input[pos];
  const type = first >> 4;
  const messageInfo = (first & 0x0f);
  pos += 1;


  // Decode the remaining length (MBI format)

  let digit;
  let remLength = 0;
  let multiplier = 1;
  do {
    if (pos === input.length) {
      return [null, startingPos];
    }
    digit = input[pos++];
    remLength += ((digit & 0x7F) * multiplier);
    multiplier *= 128;
  } while ((digit & 0x80) !== 0);

  const endPos = pos + remLength;
  if (endPos > input.length) {
    return [null, startingPos];
  }

  let wireMessage;
  switch (type) {
    case MESSAGE_TYPE.CONNACK:
      wireMessage = new WireMessage(type);
      const connectAcknowledgeFlags = input[pos++];
      const sessionPresent = connectAcknowledgeFlags & 0x01;
      if (sessionPresent) {
        wireMessage.sessionPresent = true;
      }
      wireMessage.returnCode = input[pos++];
      break;

    case MESSAGE_TYPE.PUBLISH:
      const qos: any = (messageInfo >> 1) & 0x03;

      const len = readUint16(input, pos);
      pos += 2;
      const topicName = parseUTF8(input, pos, len);
      pos += len;
      let messageIdentifier;
      // If QoS 1 or 2 there will be a messageIdentifier
      if (qos > 0) {
        messageIdentifier = readUint16(input, pos);
        pos += 2;
      }

      const message = new Message(input.subarray(pos, endPos));
      if ((messageInfo & 0x01) === 0x01) {
        message.retained = true;
      }
      if ((messageInfo & 0x08) === 0x08) {
        message.duplicate = true;
      }
      message.qos = qos;
      message.destinationName = topicName;
      return [new PublishMessage(message, messageIdentifier), endPos];

    case  MESSAGE_TYPE.PUBACK:
    case  MESSAGE_TYPE.PUBREC:
    case  MESSAGE_TYPE.PUBREL:
    case  MESSAGE_TYPE.PUBCOMP:
    case  MESSAGE_TYPE.UNSUBACK:
      wireMessage = new WireMessage(type);
      wireMessage.messageIdentifier = readUint16(input, pos);
      break;

    case  MESSAGE_TYPE.SUBACK:
      wireMessage = new WireMessage(type);
      wireMessage.messageIdentifier = readUint16(input, pos);
      pos += 2;
      wireMessage.returnCode = input.subarray(pos, endPos);
      break;

    default:
  }

  return [wireMessage, endPos];
}
