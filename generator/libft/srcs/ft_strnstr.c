/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strnstr.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/05 15:12:17 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/11 19:19:16 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <string.h>

char	*ft_strnstr(const char *haystack, const char *needle, size_t len)
{
	const char		*str;
	const char		*to_find;
	int				pos;
	int				tmp_len;

	str = haystack;
	to_find = needle;
	if (!*to_find)
		return ((char *)str);
	while (*str && len > 0)
	{
		pos = -1;
		tmp_len = 0;
		while (str[++pos] && str[pos] == to_find[pos] && len - tmp_len > 0)
			tmp_len++;
		if (to_find[pos] == '\0')
			return ((char *)str);
		str++;
		len--;
	}
	return (!*str && !*to_find ? (char *)str : NULL);
}
