const MILLION_IN_CENTS = 100_000_000n;
const BILLION_IN_CENTS = 100_000_000_000n;

interface FormattedBrlAmount {
  abbreviated: string;
  exact: string;
}

function convertDecimalToCents(value: string): bigint {
  const match = /^(\d+)(?:\.(\d+))?$/.exec(value);

  if (!match) {
    throw new Error('Valor monetário inválido.');
  }

  const integerPart = BigInt(match[1]);
  const decimalDigits = (match[2] ?? '').padEnd(3, '0');

  const cents = BigInt(decimalDigits.slice(0, 2));
  const roundUp = decimalDigits[2] >= '5' ? 1n : 0n;

  return integerPart * 100n + cents + roundUp;
}

function formatScaledAmount(
  amountInCents: bigint,
  scaleInCents: bigint,
): string {
  const scaledHundredths =
    (amountInCents * 100n + scaleInCents / 2n) /
    scaleInCents;

  const integerPart = scaledHundredths / 100n;
  const decimalPart = (scaledHundredths % 100n)
    .toString()
    .padStart(2, '0');

  return `${integerPart.toLocaleString('pt-BR')},${decimalPart}`;
}

export function formatBrlAmount(
  value: string,
): FormattedBrlAmount {
  const amountInCents = convertDecimalToCents(value);

  const integerPart = amountInCents / 100n;
  const decimalPart = (amountInCents % 100n)
    .toString()
    .padStart(2, '0');

  const exact =
    `R$ ${integerPart.toLocaleString('pt-BR')},${decimalPart}`;

  if (amountInCents >= BILLION_IN_CENTS) {
    return {
      abbreviated:
        `R$ ${formatScaledAmount(amountInCents, BILLION_IN_CENTS)} bi`,
      exact,
    };
  }

  if (amountInCents >= MILLION_IN_CENTS) {
    return {
      abbreviated:
        `R$ ${formatScaledAmount(amountInCents, MILLION_IN_CENTS)} mi`,
      exact,
    };
  }

  return {
    abbreviated: exact,
    exact,
  };
}
