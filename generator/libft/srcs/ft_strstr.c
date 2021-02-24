/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strstr.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/05 15:33:48 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/11 19:17:30 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

# include <string.h>
# include <stdio.h>

char	*ft_strstr(const char *haystack, char *needle)
{
	char	*str;
	char	*to_find;
	int		pos;

	str = (char *)haystack;
	to_find = needle;
	printf("Haystack = %s\n", str);
	printf("Needle = %s\n", needle);
	if (!*to_find)
		return (str);
	while (*str)
	{
		pos = 0;
		while (str[pos] && str[pos] == to_find[pos])
			pos++;
		if (to_find[pos] == '\0')
			return (str);
		str++;
	}
	return (NULL);
}
