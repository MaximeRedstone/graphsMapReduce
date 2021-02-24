/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strtrim.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/02 16:57:42 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/11 18:49:35 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

char	*ft_strtrim(char const *s)
{
	char					*new_s;
	unsigned int			lead;
	unsigned int			trail;

	if (!(new_s = (char *)s))
		return (NULL);
	if (!*s)
		return (ft_strnew(0));
	lead = ft_strlen(new_s) - 1;
	trail = 0;
	while ((new_s[lead - trail] == '\t' || new_s[lead - trail] == '\n'
			|| new_s[lead - trail] == ' ') && trail < lead)
		trail++;
	if (trail == lead)
		return (ft_strnew(0));
	lead = 0;
	while (new_s[lead] == '\t' || new_s[lead] == '\n' || new_s[lead] == ' ')
		lead++;
	if (!(new_s = ft_strsub(s, lead, ft_strlen((char *)s) - lead - trail)))
		return (NULL);
	return (new_s);
}
