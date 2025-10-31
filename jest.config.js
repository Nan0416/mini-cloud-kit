/**
 * @type {import('@jest/types').Config.InitialOptions}
 */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  passWithNoTests: false,
  testMatch: [
    "<rootDir>/tests/**/*.test.(js|ts)"
  ],
  collectCoverage: true,
  coverageDirectory: 'reports/coverage-reports',
  collectCoverageFrom: [
    "src/**/*.(ts|js)",
  ],
  coveragePathIgnorePatterns: [
    "index.(ts|js)",
  ],
  reporters: [
    'default',
    ['jest-junit', {
      suiteName: 'mini-cloud-kit',
      outputDirectory: 'reports/test-reports',
      outputName: 'junit.xml'
    }]
  ],
  testTimeout: 10_000,
}