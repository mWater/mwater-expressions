// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
import gulp from 'gulp';
import coffee from 'gulp-coffee';

// Compile coffeescript to js in lib/
gulp.task('coffee', () => gulp.src('./src/**/*.coffee')
  .pipe(coffee({ bare: true }))
  .pipe(gulp.dest('./lib/')));

// Copy non-coffeescript files
gulp.task('copy', () => gulp.src(['./src/**/*.js', './src/**/*.css', './src/**/*.txt', './src/**/*.d.ts'])
  .pipe(gulp.dest('./lib/')));

gulp.task("default", gulp.series("copy", "coffee"));
