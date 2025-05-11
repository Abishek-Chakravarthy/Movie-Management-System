$(document).ready(function() {
    // Fetch languages from server and populate the dropdown
    $.get("languages", function(data) {
        var languages = JSON.parse(data);
        var languageDropdown = $("#languageCollapse .card-body");
        languageDropdown.empty();
        $.each(languages, function(index, language) {
            var languageOption = $("<button>", {
                class: "btn btn-link language-option",
                text: language
            });
            languageOption.click(function() {
                // Fetch genres for the selected language
                var selectedLanguage = $(this).text();
                $.get("genres/" + selectedLanguage, function(genresData) {
                    var genres = JSON.parse(genresData);
                    var genreDropdown = $("#genreCollapse .card-body");
                    genreDropdown.empty();
                    $.each(genres, function(index, genre) {
                        var genreOption = $("<button>", {
                            class: "btn btn-link genre-option",
                            text: genre
                        });
                        genreOption.click(function() {
                            // Fetch and display movies for the selected language and genre
                            var selectedGenre = $(this).text();
                            $.get("movies/" + selectedLanguage + "/" + selectedGenre, function(moviesData) {
                                var movies = JSON.parse(moviesData);
                                var movieTableContainer = $("#movieTableContainer");
                                movieTableContainer.empty();
                                var table = $("<table>", {
                                    class: "table"
                                });
                                var tableHeader = $("<thead>").append(
                                    $("<tr>").append(
                                        $("<th>", { text: "Rating" }),
                                        $("<th>", { text: "Title" }),
                                        $("<th>", { text: "Directors" }),
                                        $("<th>", { text: "Cast" }),
                                        $("<th>", { text: "Release Date" })
                                    )
                                );
                                var tableBody = $("<tbody>");
                                $.each(movies, function(index, movie) {
                                    var row = $("<tr>").append(
                                        $("<td>", { text: movie.rating }),
                                        $("<td>", { text: movie.title }),
                                        $("<td>", { text: movie.directors }),
                                        $("<td>", { text: movie.cast }),
                                        $("<td>", { text: movie.release_date })
                                    );
                                    tableBody.append(row);
                                });
                                table.append(tableHeader, tableBody);
                                movieTableContainer.append(table);
                            });
                        });
                        genreDropdown.append(genreOption);
                    });
                });
            });
            languageDropdown.append(languageOption);
        });
    });
});
